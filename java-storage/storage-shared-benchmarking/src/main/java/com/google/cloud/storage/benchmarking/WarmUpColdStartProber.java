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

package com.google.cloud.storage.benchmarking;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

final class WarmUpColdStartProber {

  private final String project;
  private final String targetBucket;
  private final String probeBucket;
  private final String objectName;
  private final int readBytes;
  private final String api;
  private final String warmUpMode;
  private final boolean tier1Enabled;
  private final long tier1DelayMs;
  private final int workers;
  private final int rounds;
  private final PrintWriter out;

  WarmUpColdStartProber(
      String project,
      String targetBucket,
      String probeBucket,
      String objectName,
      int readBytes,
      String api,
      String warmUpMode,
      boolean tier1Enabled,
      long tier1DelayMs,
      int workers,
      int rounds,
      PrintWriter out) {
    this.project = project;
    this.targetBucket = targetBucket;
    this.probeBucket = probeBucket != null && !probeBucket.isEmpty() ? probeBucket : targetBucket;
    this.objectName = objectName;
    this.readBytes = readBytes;
    this.api = api;
    this.warmUpMode = warmUpMode;
    this.tier1Enabled = tier1Enabled;
    this.tier1DelayMs = tier1DelayMs;
    this.workers = Math.max(1, workers);
    this.rounds = Math.max(1, rounds);
    this.out = out;
  }

  void execute() throws Exception {
    boolean attemptDirectPath = "DirectPath".equalsIgnoreCase(api);
    boolean enableTier1 = tier1Enabled || "tier1".equalsIgnoreCase(warmUpMode);

    long createStartNs = System.nanoTime();
    GrpcStorageOptions options =
        StorageOptions.grpc()
            .setProjectId(project)
            .setAttemptDirectPath(attemptDirectPath)
            .setEnableGrpcClientMetrics(false)
            .setWarmUpOnCreationEnabled(enableTier1)
            .build();
    try (Storage storage = options.getService()) {
      double createMs = nanosToMillis(System.nanoTime() - createStartNs);

      if (tier1DelayMs > 0) {
        Thread.sleep(tier1DelayMs);
      }

      long warmUpStartNs = System.nanoTime();
      if ("tier2-bucketless".equalsIgnoreCase(warmUpMode)) {
        storage.warmUp().get(30, TimeUnit.SECONDS);
      } else if ("tier2-bucket".equalsIgnoreCase(warmUpMode)) {
        storage.warmUp(probeBucket).get(30, TimeUnit.SECONDS);
      } else if (!"none".equalsIgnoreCase(warmUpMode) && !"tier1".equalsIgnoreCase(warmUpMode)) {
        throw new IllegalArgumentException(
            "Unsupported -warmup_mode: "
                + warmUpMode
                + " (expected none, tier1, tier2-bucketless, or tier2-bucket)");
      }
      double warmUpMs = nanosToMillis(System.nanoTime() - warmUpStartNs);

      BlobId blobId = BlobId.of(targetBucket, objectName);
      ExecutorService pool = Executors.newFixedThreadPool(workers);
      try {
        double firstOpenMs = 0.0;
        double firstReadStepMs = 0.0;
        double firstReadTotalMs = 0.0;
        double burstWallMs = 0.0;
        List<Double> steadyOpenSamples = new ArrayList<>();
        List<Double> steadyTotalSamples = new ArrayList<>();

        for (int r = 0; r < rounds; r++) {
          RoundMetrics rm = runRound(storage, blobId, pool);
          if (r == 0) {
            firstOpenMs = rm.medianOpenMs;
            firstReadStepMs = rm.medianReadStepMs;
            firstReadTotalMs = rm.medianTotalMs;
            burstWallMs = rm.wallMs;
          } else if (r >= 3 || rounds <= 3) {
            steadyOpenSamples.add(rm.medianOpenMs);
            steadyTotalSamples.add(rm.medianTotalMs);
          }
        }

        double steadyOpenP50 = median(steadyOpenSamples);
        double steadyTotalP50 = median(steadyTotalSamples);
        double timeToFirstDataMs = createMs + tier1DelayMs + warmUpMs + firstReadTotalMs;

        out.printf(
            Locale.US,
            "{\"api\":\"%s\",\"mode\":\"%s\",\"tier1Enabled\":%s,\"tier1DelayMs\":%d,"
                + "\"workers\":%d,\"rounds\":%d,\"targetBucket\":\"%s\",\"probeBucket\":\"%s\","
                + "\"createMs\":%.2f,\"warmUpMs\":%.2f,\"firstOpenMs\":%.2f,"
                + "\"firstReadStepMs\":%.2f,\"firstReadTotalMs\":%.2f,\"burstWallMs\":%.2f,"
                + "\"steadyOpenP50Ms\":%.2f,\"steadyTotalP50Ms\":%.2f,"
                + "\"timeToFirstDataMs\":%.2f}%n",
            api,
            warmUpMode,
            enableTier1,
            tier1DelayMs,
            workers,
            rounds,
            targetBucket,
            probeBucket,
            createMs,
            warmUpMs,
            firstOpenMs,
            firstReadStepMs,
            firstReadTotalMs,
            burstWallMs,
            steadyOpenP50,
            steadyTotalP50,
            timeToFirstDataMs);
      } finally {
        pool.shutdownNow();
      }
    }
  }

  private RoundMetrics runRound(Storage storage, BlobId blobId, ExecutorService pool)
      throws Exception {
    CountDownLatch ready = new CountDownLatch(workers);
    CountDownLatch go = new CountDownLatch(1);
    List<Callable<StreamSample>> tasks = new ArrayList<>(workers);
    for (int i = 0; i < workers; i++) {
      tasks.add(
          () -> {
            ready.countDown();
            go.await();
            long t0 = System.nanoTime();
            try (BlobReadSession session =
                storage.blobReadSession(blobId).get(30, TimeUnit.SECONDS)) {
              long t1 = System.nanoTime();
              if (readBytes > 0) {
                byte[] bytes =
                    session
                        .readAs(
                            ReadProjectionConfigs.asFutureBytes()
                                .withRangeSpec(RangeSpec.of(0, readBytes)))
                        .get(30, TimeUnit.SECONDS);
                if (bytes.length == 0) {
                  throw new IllegalStateException("Read 0 bytes from " + blobId);
                }
              }
              long t2 = System.nanoTime();
              return new StreamSample(
                  nanosToMillis(t1 - t0), nanosToMillis(t2 - t1), nanosToMillis(t2 - t0));
            }
          });
    }

    List<Future<StreamSample>> futures = new ArrayList<>(workers);
    for (Callable<StreamSample> task : tasks) {
      futures.add(pool.submit(task));
    }
    ready.await(10, TimeUnit.SECONDS);
    long burstStartNs = System.nanoTime();
    go.countDown();

    List<Double> opens = new ArrayList<>(workers);
    List<Double> reads = new ArrayList<>(workers);
    List<Double> totals = new ArrayList<>(workers);
    for (Future<StreamSample> f : futures) {
      StreamSample s = f.get(60, TimeUnit.SECONDS);
      opens.add(s.openMs);
      reads.add(s.readStepMs);
      totals.add(s.totalMs);
    }
    double wallMs = nanosToMillis(System.nanoTime() - burstStartNs);
    return new RoundMetrics(median(opens), median(reads), median(totals), wallMs);
  }

  private static double nanosToMillis(long nanos) {
    return nanos / 1_000_000.0;
  }

  private static double median(List<Double> values) {
    if (values.isEmpty()) {
      return 0.0;
    }
    List<Double> copy = new ArrayList<>(values);
    Collections.sort(copy);
    int mid = copy.size() / 2;
    if (copy.size() % 2 == 1) {
      return copy.get(mid);
    }
    return (copy.get(mid - 1) + copy.get(mid)) / 2.0;
  }

  private static final class StreamSample {
    final double openMs;
    final double readStepMs;
    final double totalMs;

    StreamSample(double openMs, double readStepMs, double totalMs) {
      this.openMs = openMs;
      this.readStepMs = readStepMs;
      this.totalMs = totalMs;
    }
  }

  private static final class RoundMetrics {
    final double medianOpenMs;
    final double medianReadStepMs;
    final double medianTotalMs;
    final double wallMs;

    RoundMetrics(
        double medianOpenMs, double medianReadStepMs, double medianTotalMs, double wallMs) {
      this.medianOpenMs = medianOpenMs;
      this.medianReadStepMs = medianReadStepMs;
      this.medianTotalMs = medianTotalMs;
      this.wallMs = wallMs;
    }
  }
}
