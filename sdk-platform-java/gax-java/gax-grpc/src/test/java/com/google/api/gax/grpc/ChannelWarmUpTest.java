/*
 * Copyright 2026 Google LLC
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google LLC nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.google.api.gax.grpc;

import static com.google.api.gax.grpc.InstantiatingGrpcChannelProvider.GCE_PRODUCTION_NAME_AFTER_2016;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.api.core.ApiFuture;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider.HardBoundTokenTypes;
import com.google.api.gax.grpc.testing.FakeChannelFactory;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.internal.EnvironmentProvider;
import com.google.api.gax.rpc.mtls.CertificateBasedAccess;
import com.google.auth.Credentials;
import com.google.auth.RequestMetadataCallback;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ChannelWarmUpTest {
  private static String originalOsName;

  private ScheduledExecutorService executor;
  private ChannelPool pool;
  private CertificateBasedAccess certificateBasedAccess;

  @BeforeAll
  static void saveOsName() {
    originalOsName = System.getProperty("os.name");
  }

  @BeforeEach
  void setUp() {
    executor = Executors.newSingleThreadScheduledExecutor();
    certificateBasedAccess =
        new CertificateBasedAccess(
            name -> name.equals("GOOGLE_API_USE_MTLS_ENDPOINT") ? "never" : "false");
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    System.setProperty("os.name", originalOsName);
    if (pool != null) {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
    executor.shutdownNow();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  void requestConnection_toleratesUnsupportedOperationException() {
    ManagedChannel unsupportedChannel = mock(ManagedChannel.class);
    when(unsupportedChannel.getState(true)).thenThrow(new UnsupportedOperationException());

    ChannelWarmUp.requestConnection(unsupportedChannel);

    verify(unsupportedChannel, times(1)).getState(true);
  }

  @Test
  void warmUp_waitsForChannelTransitionFromIdleThroughConnectingToReady() throws Exception {
    ManagedChannel channel1 = mock(ManagedChannel.class);
    ManagedChannel channel2 = mock(ManagedChannel.class);
    when(channel1.authority()).thenReturn("storage.googleapis.com");
    when(channel2.authority()).thenReturn("storage.googleapis.com");

    AtomicReference<Runnable> idleCallback = new AtomicReference<>();
    AtomicReference<Runnable> connectingCallback = new AtomicReference<>();

    when(channel1.getState(true)).thenReturn(ConnectivityState.IDLE);
    when(channel1.getState(false))
        .thenReturn(ConnectivityState.CONNECTING)
        .thenReturn(ConnectivityState.READY);
    doAnswer(
            inv -> {
              idleCallback.set(inv.getArgument(1));
              return null;
            })
        .when(channel1)
        .notifyWhenStateChanged(eq(ConnectivityState.IDLE), any(Runnable.class));
    doAnswer(
            inv -> {
              connectingCallback.set(inv.getArgument(1));
              return null;
            })
        .when(channel1)
        .notifyWhenStateChanged(eq(ConnectivityState.CONNECTING), any(Runnable.class));

    when(channel2.getState(true)).thenReturn(ConnectivityState.READY);

    pool =
        ChannelPool.create(
            ChannelPoolSettings.staticallySized(2),
            new FakeChannelFactory(Arrays.asList(channel1, channel2)),
            executor,
            null);

    ChannelWarmUp warmUp =
        new ChannelWarmUp(pool, null, URI.create("https://storage.googleapis.com"), executor);
    ApiFuture<Void> future = warmUp.warmUp();
    assertThat(future.isDone()).isFalse();

    idleCallback.get().run();
    assertThat(future.isDone()).isFalse();

    connectingCallback.get().run();
    assertThat(future.get(5, TimeUnit.SECONDS)).isNull();
  }

  @Test
  void warmUp_failsWhenChannelEntersTransientFailure() throws Exception {
    ManagedChannel channel = mock(ManagedChannel.class);
    when(channel.authority()).thenReturn("storage.googleapis.com");
    AtomicReference<Runnable> callbackRef = new AtomicReference<>();
    when(channel.getState(true)).thenReturn(ConnectivityState.CONNECTING);
    when(channel.getState(false)).thenReturn(ConnectivityState.TRANSIENT_FAILURE);
    doAnswer(
            inv -> {
              callbackRef.set(inv.getArgument(1));
              return null;
            })
        .when(channel)
        .notifyWhenStateChanged(eq(ConnectivityState.CONNECTING), any(Runnable.class));

    pool =
        ChannelPool.create(
            ChannelPoolSettings.staticallySized(1),
            new FakeChannelFactory(Collections.singletonList(channel)),
            executor,
            null);

    ChannelWarmUp warmUp =
        new ChannelWarmUp(pool, null, URI.create("https://storage.googleapis.com"), executor);
    ApiFuture<Void> future = warmUp.warmUp();
    callbackRef.get().run();

    ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
    assertThat(thrown.getCause()).isInstanceOf(StatusRuntimeException.class);
    assertThat(((StatusRuntimeException) thrown.getCause()).getStatus().getCode())
        .isEqualTo(Status.Code.UNAVAILABLE);
  }

  @Test
  void warmUp_timesOutWhenChannelNeverLeavesConnecting() throws Exception {
    ManagedChannel channel = mock(ManagedChannel.class);
    when(channel.authority()).thenReturn("storage.googleapis.com");
    when(channel.getState(true)).thenReturn(ConnectivityState.CONNECTING);

    pool =
        ChannelPool.create(
            ChannelPoolSettings.staticallySized(1),
            new FakeChannelFactory(Collections.singletonList(channel)),
            executor,
            null);

    ChannelWarmUp warmUp =
        new ChannelWarmUp(
            pool,
            null,
            URI.create("https://storage.googleapis.com"),
            executor,
            Duration.ofMillis(50));

    ExecutionException thrown =
        assertThrows(ExecutionException.class, () -> warmUp.warmUp().get(5, TimeUnit.SECONDS));
    assertThat(thrown.getCause()).isInstanceOf(StatusRuntimeException.class);
    assertThat(((StatusRuntimeException) thrown.getCause()).getStatus().getCode())
        .isEqualTo(Status.Code.DEADLINE_EXCEEDED);
  }

  @Test
  void warmUp_deduplicatesInFlightCredentialPrefetchAndRetriesAfterFailure() throws Exception {
    ManagedChannel channel = mock(ManagedChannel.class);
    when(channel.authority()).thenReturn("storage.googleapis.com");
    when(channel.getState(true)).thenReturn(ConnectivityState.READY);

    pool =
        ChannelPool.create(
            ChannelPoolSettings.staticallySized(1),
            new FakeChannelFactory(Collections.singletonList(channel)),
            executor,
            null);

    AtomicInteger fetchCount = new AtomicInteger();
    AtomicReference<RequestMetadataCallback> pendingCallback = new AtomicReference<>();
    Credentials creds = mock(Credentials.class, withSettings().withoutAnnotations());
    when(creds.hasRequestMetadata()).thenReturn(true);
    doAnswer(
            inv -> {
              int attempt = fetchCount.incrementAndGet();
              RequestMetadataCallback cb = inv.getArgument(2);
              if (attempt == 1) {
                pendingCallback.set(cb);
              } else {
                cb.onSuccess(ImmutableMap.of("Authorization", ImmutableList.of("Bearer tok")));
              }
              return null;
            })
        .when(creds)
        .getRequestMetadata(any(), any(Executor.class), any(RequestMetadataCallback.class));

    ChannelWarmUp warmUp =
        new ChannelWarmUp(pool, creds, URI.create("https://storage.googleapis.com"), executor);

    // Background start + concurrent warmUp() share the same single-flight credential fetch.
    warmUp.startInBackground();
    ApiFuture<Void> firstWarmUp = warmUp.warmUp();
    assertThat(fetchCount.get()).isEqualTo(1);
    assertThat(firstWarmUp.isDone()).isFalse();

    // Fail the first fetch; a subsequent warmUp() retries instead of caching the failure forever.
    pendingCallback.get().onFailure(new IOException("Transient metadata timeout"));
    ExecutionException thrown = assertThrows(ExecutionException.class, firstWarmUp::get);
    assertThat(thrown.getCause()).isInstanceOf(IOException.class);

    ApiFuture<Void> retryWarmUp = warmUp.warmUp();
    assertThat(retryWarmUp.get(5, TimeUnit.SECONDS)).isNull();
    assertThat(fetchCount.get()).isEqualTo(2);

    // Once succeeded, another warmUp() reuses the cached success without re-fetching.
    assertThat(warmUp.warmUp().get(5, TimeUnit.SECONDS)).isNull();
    assertThat(fetchCount.get()).isEqualTo(2);
  }

  @Test
  void provider_warmUpOnCreationDisabledByDefault_doesNotPrefetchCredentialsDuringCreate()
      throws Exception {
    Credentials creds = mock(Credentials.class, withSettings().withoutAnnotations());
    when(creds.hasRequestMetadata()).thenReturn(true);

    InstantiatingGrpcChannelProvider provider =
        InstantiatingGrpcChannelProvider.newBuilder()
            .setCertificateBasedAccess(certificateBasedAccess)
            .setEndpoint("localhost:8080")
            .setHeaderProvider(FixedHeaderProvider.create(Collections.emptyMap()))
            .setCredentials(creds)
            .setBackgroundExecutor(executor)
            .build();

    assertThat(provider.isWarmUpOnCreation()).isFalse();
    GrpcTransportChannel transportChannel = (GrpcTransportChannel) provider.getTransportChannel();
    try {
      verify(creds, never())
          .getRequestMetadata(any(), any(Executor.class), any(RequestMetadataCallback.class));
    } finally {
      transportChannel.shutdownNow();
    }
  }

  @Test
  void provider_warmUpOnCreationEnabled_prefetchesCredentialsAndPreservesFlagAcrossToBuilder()
      throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Credentials creds = mock(Credentials.class, withSettings().withoutAnnotations());
    when(creds.hasRequestMetadata()).thenReturn(true);
    doAnswer(
            inv -> {
              RequestMetadataCallback cb = inv.getArgument(2);
              cb.onSuccess(Collections.emptyMap());
              latch.countDown();
              return null;
            })
        .when(creds)
        .getRequestMetadata(any(), any(Executor.class), any(RequestMetadataCallback.class));

    InstantiatingGrpcChannelProvider baseProvider =
        InstantiatingGrpcChannelProvider.newBuilder()
            .setCertificateBasedAccess(certificateBasedAccess)
            .setEndpoint("storage.googleapis.com:443")
            .setHeaderProvider(FixedHeaderProvider.create(Collections.emptyMap()))
            .setBackgroundExecutor(executor)
            .setWarmUpOnCreation(true)
            .build();

    InstantiatingGrpcChannelProvider providerWithCreds =
        (InstantiatingGrpcChannelProvider) baseProvider.withCredentials(creds);
    assertThat(providerWithCreds.isWarmUpOnCreation()).isTrue();
    assertThat(providerWithCreds.getMetadataUri())
        .isEqualTo(URI.create("https://storage.googleapis.com"));

    GrpcTransportChannel transportChannel =
        (GrpcTransportChannel) providerWithCreds.getTransportChannel();
    try {
      assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      transportChannel.shutdownNow();
    }
  }

  @Test
  void provider_resolveActiveCredentials_usesAltsBoundCredentialsOnDirectPath() {
    System.setProperty("os.name", "Linux");
    EnvironmentProvider envProvider =
        mock(EnvironmentProvider.class, withSettings().withoutAnnotations());
    when(envProvider.getenv(InstantiatingGrpcChannelProvider.DIRECT_PATH_ENV_DISABLE_DIRECT_PATH))
        .thenReturn("false");

    ComputeEngineCredentials baseGceCreds = ComputeEngineCredentials.create();

    InstantiatingGrpcChannelProvider.Builder directPathBuilder =
        InstantiatingGrpcChannelProvider.newBuilder()
            .setCertificateBasedAccess(certificateBasedAccess)
            .setAttemptDirectPath(true)
            .setAttemptDirectPathXds()
            .setAllowHardBoundTokenTypes(Collections.singletonList(HardBoundTokenTypes.ALTS))
            .setCredentials(baseGceCreds)
            .setEndpoint("storage.googleapis.com:443")
            .setEnvProvider(envProvider);

    // Trigger build() so altsCredentials is populated, then force GCE product name for DirectPath.
    InstantiatingGrpcChannelProvider builtOnce = directPathBuilder.build();
    InstantiatingGrpcChannelProvider directPathProvider =
        new InstantiatingGrpcChannelProvider(builtOnce.toBuilder(), GCE_PRODUCTION_NAME_AFTER_2016);

    assertThat(directPathProvider.canUseDirectPath()).isTrue();
    Credentials activeDirectPathCreds = directPathProvider.resolveActiveCredentials();
    assertThat(activeDirectPathCreds).isInstanceOf(ComputeEngineCredentials.class);
    assertThat(activeDirectPathCreds).isNotSameInstanceAs(baseGceCreds);

    // On non-GCE (CloudPath), resolveActiveCredentials falls back to the base credentials.
    InstantiatingGrpcChannelProvider cloudPathProvider =
        new InstantiatingGrpcChannelProvider(builtOnce.toBuilder(), "not-gce");
    assertThat(cloudPathProvider.canUseDirectPath()).isFalse();
    assertThat(cloudPathProvider.resolveActiveCredentials()).isSameInstanceAs(baseGceCreds);
  }
}
