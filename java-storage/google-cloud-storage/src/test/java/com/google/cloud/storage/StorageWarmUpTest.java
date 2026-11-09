/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.storage;

import static com.google.cloud.storage.TestUtils.GRPC_STATUS_DETAILS_KEY;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.core.ApiFuture;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage.BucketSourceOption;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.storage.v2.BidiReadHandle;
import com.google.storage.v2.BidiReadObjectRedirectedError;
import com.google.storage.v2.BidiReadObjectRequest;
import com.google.storage.v2.BidiReadObjectResponse;
import com.google.storage.v2.BidiReadObjectSpec;
import com.google.storage.v2.BucketName;
import com.google.storage.v2.GetBucketRequest;
import com.google.storage.v2.Object;
import com.google.storage.v2.StorageGrpc.StorageImplBase;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class StorageWarmUpTest {

  private static final Context.Key<Metadata> CAPTURED_HEADERS_KEY = Context.key("captured-headers");
  private static final Metadata.Key<String> X_GOOG_REQUEST_PARAMS =
      Metadata.Key.of("x-goog-request-params", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> X_GOOG_USER_PROJECT =
      Metadata.Key.of("x-goog-user-project", Metadata.ASCII_STRING_MARSHALLER);

  private InMemorySpanExporter spanExporter;
  private InMemoryMetricReader metricReader;
  private SdkTracerProvider tracerProvider;
  private SdkMeterProvider meterProvider;
  private OpenTelemetrySdk openTelemetry;
  private boolean originalAcoEnabled;

  @Before
  public void setUp() {
    originalAcoEnabled = OtelStorageDecorator.acoEnabled;
    OtelStorageDecorator.acoEnabled = false;
    spanExporter = InMemorySpanExporter.create();
    metricReader = InMemoryMetricReader.create();
    tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
    meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
    openTelemetry =
        OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setMeterProvider(meterProvider)
            .build();
  }

  @After
  public void tearDown() {
    OtelStorageDecorator.acoEnabled = originalAcoEnabled;
    tracerProvider.close();
    meterProvider.close();
  }

  @Test
  public void httpTransport_warmUpIsImmediateNoOpAndValidatesBucket() throws Exception {
    try (Storage httpStorage =
        StorageOptions.http()
            .setProjectId("test-proj")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService()) {
      ApiFuture<Void> bucketless = httpStorage.warmUp();
      assertThat(bucketless.isDone()).isTrue();
      assertThat(bucketless.get()).isNull();

      ApiFuture<Void> targeted = httpStorage.warmUp("my-bucket");
      assertThat(targeted.isDone()).isTrue();
      assertThat(targeted.get()).isNull();

      assertThrows(NullPointerException.class, () -> httpStorage.warmUp(null));
      assertThrows(IllegalArgumentException.class, () -> httpStorage.warmUp(""));
    }
  }

  @Test
  public void grpcWarmUp_bucketless_connectsChannelAndPrefetchesCredentials() throws Exception {
    AtomicInteger refreshCalls = new AtomicInteger();
    GoogleCredentials creds =
        new GoogleCredentials() {
          @Override
          public AccessToken refreshAccessToken() {
            refreshCalls.incrementAndGet();
            return new AccessToken("token-1", new Date(System.currentTimeMillis() + 3_600_000L));
          }
        };

    try (TestGrpcHarness harness =
            TestGrpcHarness.start(
                (req, respond) -> respond.onError(Status.NOT_FOUND.asRuntimeException()));
        Storage storage =
            harness
                .baseOptionsBuilder(openTelemetry, meterProvider)
                .setCredentials(creds)
                .build()
                .getService()) {
      assertThat(refreshCalls.get()).isEqualTo(0);

      storage.warmUp().get(5, TimeUnit.SECONDS);

      assertThat(refreshCalls.get()).isEqualTo(1);
      SpanData span = singleWarmUpSpan();
      assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
      assertThat(span.getAttributes().get(AttributeKey.stringKey("gsutil.uri"))).isEqualTo("gs://");
      assertWarmUpMetricOutcome("OK");
    }
  }

  @Test
  public void grpcWarmUp_bucketProbeNotFound_completesCleanlyWithOkSpan() throws Exception {
    try (TestGrpcHarness harness =
            TestGrpcHarness.start(
                (req, respond) -> respond.onError(Status.NOT_FOUND.asRuntimeException()));
        Storage storage =
            harness.baseOptionsBuilder(openTelemetry, meterProvider).build().getService()) {
      Void result = storage.warmUp("test-bucket").get(5, TimeUnit.SECONDS);
      assertThat(result).isNull();

      assertThat(harness.requests).hasSize(1);
      BidiReadObjectRequest sent = harness.requests.get(0);
      assertThat(sent.getReadRangesList()).isEmpty();
      BidiReadObjectSpec spec = sent.getReadObjectSpec();
      assertThat(spec.getBucket()).isEqualTo("projects/_/buckets/test-bucket");
      assertThat(spec.getObject()).isEqualTo("__gcs_sdk_warmup_nonexistent_object__");
      assertThat(spec.getGeneration()).isEqualTo(1L);

      Metadata headers = harness.capturedHeaders.get(0);
      assertThat(ImmutableList.copyOf(headers.getAll(X_GOOG_REQUEST_PARAMS)))
          .containsExactly("bucket=projects/_/buckets/test-bucket");

      SpanData span = singleWarmUpSpan();
      assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
      assertThat(span.getEvents()).isEmpty();
      assertThat(span.getAttributes().get(AttributeKey.stringKey("gsutil.uri")))
          .isEqualTo("gs://test-bucket/");
      assertWarmUpMetricOutcome("NOT_FOUND");
    }
  }

  @Test
  public void grpcWarmUp_unexpectedOkResponse_closesStreamWithoutLeakingZeroCopyBuffers()
      throws Exception {
    BidiReadObjectResponse openResponse =
        BidiReadObjectResponse.newBuilder()
            .setMetadata(
                Object.newBuilder()
                    .setBucket(BucketName.format("_", "test-bucket"))
                    .setName("__gcs_sdk_warmup_nonexistent_object__")
                    .setGeneration(1L)
                    .setSize(128L)
                    .build())
            .build();

    try (TestGrpcHarness harness =
            TestGrpcHarness.start((req, respond) -> respond.onNext(openResponse));
        Storage storage =
            harness.baseOptionsBuilder(openTelemetry, meterProvider).build().getService()) {
      storage.warmUp("test-bucket").get(5, TimeUnit.SECONDS);

      OtelStorageDecorator decorator = (OtelStorageDecorator) storage;
      GrpcStorageImpl impl = (GrpcStorageImpl) decorator.delegate;
      Field stubField = impl.storageClient.getClass().getSuperclass().getDeclaredField("stub");
      stubField.setAccessible(true);
      java.lang.Object stub = stubField.get(impl.storageClient);
      Field marshallerField = stub.getClass().getDeclaredField("bidiReadObjectResponseMarshaller");
      marshallerField.setAccessible(true);
      GrpcStorageOptions.ZeroCopyResponseMarshaller<?> marshaller =
          (GrpcStorageOptions.ZeroCopyResponseMarshaller<?>) marshallerField.get(stub);
      Field unclosedField =
          GrpcStorageOptions.ZeroCopyResponseMarshaller.class.getDeclaredField("unclosedStreams");
      unclosedField.setAccessible(true);
      Map<?, ?> unclosedStreams = (Map<?, ?>) unclosedField.get(marshaller);
      assertThat(unclosedStreams).isEmpty();

      SpanData span = singleWarmUpSpan();
      assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
      assertWarmUpMetricOutcome("OK");
    }
  }

  @Test
  public void grpcWarmUp_redirectedError_retriesWithRoutingTokenAndSucceeds() throws Exception {
    String routingToken = "route-tok-" + UUID.randomUUID();
    BidiReadHandle readHandle =
        BidiReadHandle.newBuilder().setHandle(ByteString.copyFromUtf8("handle-1")).build();
    AtomicInteger callCount = new AtomicInteger();

    try (TestGrpcHarness harness =
            TestGrpcHarness.start(
                (req, respond) -> {
                  if (callCount.incrementAndGet() == 1) {
                    BidiReadObjectRedirectedError redirect =
                        BidiReadObjectRedirectedError.newBuilder()
                            .setRoutingToken(routingToken)
                            .setReadHandle(readHandle)
                            .build();
                    com.google.rpc.Status statusProto =
                        com.google.rpc.Status.newBuilder()
                            .setCode(com.google.rpc.Code.UNAVAILABLE_VALUE)
                            .setMessage("redirect")
                            .addDetails(Any.pack(redirect))
                            .build();
                    Metadata trailers = new Metadata();
                    trailers.put(GRPC_STATUS_DETAILS_KEY, statusProto);
                    respond.onError(
                        Status.UNAVAILABLE
                            .withDescription("redirect")
                            .asRuntimeException(trailers));
                  } else {
                    respond.onError(Status.NOT_FOUND.asRuntimeException());
                  }
                });
        Storage storage =
            harness.baseOptionsBuilder(openTelemetry, meterProvider).build().getService()) {
      storage.warmUp("test-bucket").get(5, TimeUnit.SECONDS);

      assertThat(harness.requests).hasSize(2);
      assertThat(harness.requests.get(1).getReadObjectSpec().getRoutingToken())
          .isEqualTo(routingToken);
      assertThat(ImmutableList.copyOf(harness.capturedHeaders.get(1).getAll(X_GOOG_REQUEST_PARAMS)))
          .containsExactly("bucket=projects/_/buckets/test-bucket&routing_token=" + routingToken);
      assertWarmUpMetricOutcome("NOT_FOUND");
    }
  }

  @Test
  public void grpcWarmUp_transientUnavailableRetried_andNonRetryableErrorsFail() throws Exception {
    AtomicInteger transientCount = new AtomicInteger();
    try (TestGrpcHarness harness =
            TestGrpcHarness.start(
                (req, respond) -> {
                  if (transientCount.incrementAndGet() == 1) {
                    respond.onError(Status.UNAVAILABLE.asRuntimeException());
                  } else {
                    respond.onError(Status.NOT_FOUND.asRuntimeException());
                  }
                });
        Storage storage =
            harness.baseOptionsBuilder(openTelemetry, meterProvider).build().getService()) {
      storage.warmUp("test-bucket").get(5, TimeUnit.SECONDS);
      assertThat(transientCount.get()).isEqualTo(2);
    }

    assertProbeFailure(Status.PERMISSION_DENIED, 403, "PERMISSION_DENIED");
    assertProbeFailure(Status.UNAUTHENTICATED, 401, "UNAUTHENTICATED");
    assertProbeFailure(Status.INVALID_ARGUMENT, 400, "INVALID_ARGUMENT");
    assertProbeFailure(Status.UNAVAILABLE, 503, "UNAVAILABLE");
  }

  @Test
  public void grpcWarmUp_userProjectAndQuotaProjectPrecedenceWithoutDuplicateHeaders()
      throws Exception {
    GoogleCredentials credsWithQuota =
        new GoogleCredentials(GoogleCredentials.newBuilder().setQuotaProjectId("cred-quota-proj")) {
          @Override
          public AccessToken refreshAccessToken() {
            return new AccessToken("tok", new Date(System.currentTimeMillis() + 3_600_000L));
          }
        };

    try (TestGrpcHarness harness =
            TestGrpcHarness.start(
                (req, respond) -> respond.onError(Status.NOT_FOUND.asRuntimeException()));
        Storage storage =
            harness
                .baseOptionsBuilder(openTelemetry, meterProvider)
                .setCredentials(credsWithQuota)
                .build()
                .getService()) {
      storage.warmUp("test-bucket").get(5, TimeUnit.SECONDS);
      storage
          .warmUp("test-bucket", BucketSourceOption.userProject("explicit-user-proj"))
          .get(5, TimeUnit.SECONDS);

      assertThat(harness.capturedHeaders).hasSize(2);
      assertThat(ImmutableList.copyOf(harness.capturedHeaders.get(0).getAll(X_GOOG_USER_PROJECT)))
          .containsExactly("cred-quota-proj");
      assertThat(ImmutableList.copyOf(harness.capturedHeaders.get(1).getAll(X_GOOG_USER_PROJECT)))
          .containsExactly("explicit-user-proj");
    }
  }

  @Test
  public void grpcWarmUp_invalidBucketArguments_throwAndRecordSpanError() throws Exception {
    try (TestGrpcHarness harness =
            TestGrpcHarness.start(
                (req, respond) -> respond.onError(Status.NOT_FOUND.asRuntimeException()));
        Storage storage =
            harness.baseOptionsBuilder(openTelemetry, meterProvider).build().getService()) {
      assertThrows(NullPointerException.class, () -> storage.warmUp(null));
      assertThrows(IllegalArgumentException.class, () -> storage.warmUp(""));

      List<SpanData> spans = spanExporter.getFinishedSpanItems();
      assertThat(spans).hasSize(2);
      for (SpanData span : spans) {
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
      }
    }
  }

  private void assertProbeFailure(Status grpcStatus, int expectedHttpCode, String expectedOutcome)
      throws Exception {
    spanExporter.reset();
    try (InMemoryMetricReader localReader = InMemoryMetricReader.create();
        SdkMeterProvider localMeterProvider =
            SdkMeterProvider.builder().registerMetricReader(localReader).build();
        TestGrpcHarness harness =
            TestGrpcHarness.start(
                (req, respond) -> respond.onError(grpcStatus.asRuntimeException()));
        Storage storage =
            harness
                .baseOptionsBuilder(openTelemetry, localMeterProvider)
                .setRetrySettings(
                    RetrySettings.newBuilder()
                        .setMaxAttempts(2)
                        .setInitialRetryDelayDuration(Duration.ofMillis(20))
                        .setRetryDelayMultiplier(1.0)
                        .setMaxRetryDelayDuration(Duration.ofMillis(20))
                        .setTotalTimeoutDuration(Duration.ofSeconds(2))
                        .build())
                .build()
                .getService()) {
      ExecutionException thrown =
          assertThrows(
              ExecutionException.class,
              () -> storage.warmUp("test-bucket").get(5, TimeUnit.SECONDS));
      assertThat(thrown.getCause()).isInstanceOf(StorageException.class);
      StorageException se = (StorageException) thrown.getCause();
      assertThat(se.getCode()).isEqualTo(expectedHttpCode);

      SpanData span = singleWarmUpSpan();
      assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
      assertWarmUpMetricOutcome(localReader, expectedOutcome);
    }
  }

  private SpanData singleWarmUpSpan() {
    List<SpanData> warmUpSpans =
        spanExporter.getFinishedSpanItems().stream()
            .filter(s -> s.getName().endsWith("/warmUp"))
            .collect(Collectors.toList());
    assertThat(warmUpSpans).hasSize(1);
    return warmUpSpans.get(0);
  }

  private void assertWarmUpMetricOutcome(String expectedOutcome) {
    assertWarmUpMetricOutcome(metricReader, expectedOutcome);
  }

  private static void assertWarmUpMetricOutcome(
      InMemoryMetricReader reader, String expectedOutcome) {
    Collection<MetricData> metrics = reader.collectAllMetrics();
    Map<String, MetricData> byName =
        metrics.stream().collect(Collectors.toMap(MetricData::getName, m -> m));
    assertThat(byName).containsKey(StorageClientMetrics.METRIC_GCP_STORAGE_CLIENT_WARMUP_DURATION);
    MetricData ops = byName.get(StorageClientMetrics.METRIC_GCP_STORAGE_CLIENT_WARMUP_OPERATIONS);
    assertThat(ops).isNotNull();
    List<String> outcomes = new ArrayList<>();
    for (LongPointData point : ops.getLongSumData().getPoints()) {
      outcomes.add(point.getAttributes().get(StorageClientMetrics.KEY_GCP_STORAGE_WARMUP_OUTCOME));
    }
    assertThat(outcomes).contains(expectedOutcome);
  }

  private static final class TestGrpcHarness implements AutoCloseable {
    final Server server;
    final String host;
    final List<BidiReadObjectRequest> requests;
    final List<Metadata> capturedHeaders;

    private TestGrpcHarness(
        Server server,
        String host,
        List<BidiReadObjectRequest> requests,
        List<Metadata> capturedHeaders) {
      this.server = server;
      this.host = host;
      this.requests = requests;
      this.capturedHeaders = capturedHeaders;
    }

    static TestGrpcHarness start(
        BiConsumer<BidiReadObjectRequest, StreamObserver<BidiReadObjectResponse>> handler)
        throws IOException {
      Server server;
      List<BidiReadObjectRequest> reqList = new CopyOnWriteArrayList<>();
      List<Metadata> headerList = Collections.synchronizedList(new ArrayList<>());

      StorageImplBase service =
          new StorageImplBase() {
            @Override
            public StreamObserver<BidiReadObjectRequest> bidiReadObject(
                StreamObserver<BidiReadObjectResponse> responseObserver) {
              Metadata callHeaders = CAPTURED_HEADERS_KEY.get();
              headerList.add(callHeaders != null ? callHeaders : new Metadata());
              return new StreamObserver<BidiReadObjectRequest>() {
                @Override
                public void onNext(BidiReadObjectRequest value) {
                  reqList.add(value);
                  handler.accept(value, responseObserver);
                }

                @Override
                public void onError(Throwable t) {}

                @Override
                public void onCompleted() {
                  try {
                    responseObserver.onCompleted();
                  } catch (StatusRuntimeException | IllegalStateException ignored) {
                    // Already closed by handler.onError
                  }
                }
              };
            }

            @Override
            public void getBucket(
                GetBucketRequest request,
                StreamObserver<com.google.storage.v2.Bucket> responseObserver) {
              responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            }
          };

      ServerInterceptor headerCaptor =
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
              Metadata copy = new Metadata();
              copy.merge(headers);
              Context ctx = Context.current().withValue(CAPTURED_HEADERS_KEY, copy);
              return Contexts.interceptCall(ctx, call, headers, next);
            }
          };

      InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
      server =
          NettyServerBuilder.forAddress(addr)
              .addService(service)
              .intercept(headerCaptor)
              .build()
              .start();
      String host = String.format(Locale.US, "http://127.0.0.1:%d", server.getPort());
      return new TestGrpcHarness(server, host, reqList, headerList);
    }

    GrpcStorageOptions.Builder baseOptionsBuilder(
        OpenTelemetrySdk otel, SdkMeterProvider meterProv) {
      return StorageOptions.grpc()
          .setHost(host)
          .setProjectId("test-proj")
          .setCredentials(NoCredentials.getInstance())
          .setAttemptDirectPath(false)
          .setEnableGrpcClientMetrics(false)
          .setOpenTelemetry(otel)
          .setEnableOtelMetrics(true)
          .setMeterProvider(meterProv)
          .setRetrySettings(
              RetrySettings.newBuilder()
                  .setMaxAttempts(4)
                  .setInitialRetryDelayDuration(Duration.ofMillis(20))
                  .setRetryDelayMultiplier(1.2)
                  .setMaxRetryDelayDuration(Duration.ofMillis(100))
                  .setTotalTimeoutDuration(Duration.ofSeconds(5))
                  .build());
    }

    @Override
    public void close() throws InterruptedException {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
