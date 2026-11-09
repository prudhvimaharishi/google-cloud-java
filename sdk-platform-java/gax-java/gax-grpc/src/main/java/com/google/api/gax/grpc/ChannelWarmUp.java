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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.auth.Credentials;
import com.google.auth.RequestMetadataCallback;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Coordinates background channel connection initiation and credential token prefetching for a
 * {@link ChannelPool}.
 */
@NullMarked
final class ChannelWarmUp {
  static final Duration DEFAULT_CHANNEL_READY_TIMEOUT = Duration.ofSeconds(15);

  private final ChannelPool channelPool;
  private final @Nullable Credentials activeCredentials;
  private final @Nullable URI metadataUri;
  private final ScheduledExecutorService executor;
  private final Duration channelReadyTimeout;
  private final Object credentialLock = new Object();
  private @Nullable ApiFuture<Void> credentialPrefetchFuture;

  ChannelWarmUp(
      ChannelPool channelPool,
      @Nullable Credentials activeCredentials,
      @Nullable URI metadataUri,
      ScheduledExecutorService executor) {
    this(channelPool, activeCredentials, metadataUri, executor, DEFAULT_CHANNEL_READY_TIMEOUT);
  }

  @VisibleForTesting
  ChannelWarmUp(
      ChannelPool channelPool,
      @Nullable Credentials activeCredentials,
      @Nullable URI metadataUri,
      ScheduledExecutorService executor,
      Duration channelReadyTimeout) {
    this.channelPool = channelPool;
    this.activeCredentials = activeCredentials;
    this.metadataUri = metadataUri;
    this.executor = executor;
    this.channelReadyTimeout = channelReadyTimeout;
  }

  /** Kicks off channel connections and credential token prefetching without blocking. */
  void startInBackground() {
    channelPool.requestConnections();
    ensureCredentialPrefetchStarted();
  }

  /**
   * Initiates channel connections and credential token prefetching (if not already started) and
   * returns a future that completes when both finish.
   */
  ApiFuture<Void> warmUp() {
    ApiFuture<Void> credentialsFuture = ensureCredentialPrefetchStarted();
    ApiFuture<Void> channelsFuture = channelPool.awaitReady(channelReadyTimeout);
    return ApiFutures.transform(
        ApiFutures.allAsList(ImmutableList.of(credentialsFuture, channelsFuture)),
        ignored -> null,
        MoreExecutors.directExecutor());
  }

  static void requestConnection(ManagedChannel channel) {
    try {
      channel.getState(true);
    } catch (UnsupportedOperationException ignored) {
      // In-process and custom channels that do not track connectivity state need no priming.
    }
  }

  static ApiFuture<Void> awaitChannelReady(
      ManagedChannel channel, ScheduledExecutorService executor, Duration timeout) {
    final ConnectivityState initialState;
    try {
      initialState = channel.getState(true);
    } catch (UnsupportedOperationException ignored) {
      return ApiFutures.immediateFuture(null);
    }

    // Unstubbed test mocks may return null despite gRPC's non-null contract.
    if (initialState == null || initialState == ConnectivityState.READY) {
      return ApiFutures.immediateFuture(null);
    }
    if (initialState == ConnectivityState.TRANSIENT_FAILURE) {
      return ApiFutures.immediateFailedFuture(
          Status.UNAVAILABLE
              .withDescription("Channel entered TRANSIENT_FAILURE during warm-up")
              .asRuntimeException());
    }
    if (initialState == ConnectivityState.SHUTDOWN) {
      return ApiFutures.immediateFailedFuture(
          Status.UNAVAILABLE.withDescription("Channel is shut down").asRuntimeException());
    }

    SettableApiFuture<Void> readyFuture = SettableApiFuture.create();
    final ScheduledFuture<?> timeoutFuture;
    try {
      timeoutFuture =
          executor.schedule(
              () ->
                  readyFuture.setException(
                      Status.DEADLINE_EXCEEDED
                          .withDescription("Timed out waiting for gRPC channel to become READY")
                          .asRuntimeException()),
              timeout.toNanos(),
              TimeUnit.NANOSECONDS);
    } catch (RejectedExecutionException e) {
      return ApiFutures.immediateFailedFuture(
          Status.UNAVAILABLE
              .withDescription("Background executor shut down during channel warm-up")
              .withCause(e)
              .asRuntimeException());
    }
    readyFuture.addListener(() -> timeoutFuture.cancel(false), MoreExecutors.directExecutor());

    observeStateChange(channel, initialState, readyFuture);
    return readyFuture;
  }

  private static void observeStateChange(
      ManagedChannel channel, ConnectivityState sourceState, SettableApiFuture<Void> readyFuture) {
    try {
      channel.notifyWhenStateChanged(sourceState, () -> onStateChanged(channel, readyFuture));
    } catch (UnsupportedOperationException ignored) {
      readyFuture.set(null);
    }
  }

  private static void onStateChanged(ManagedChannel channel, SettableApiFuture<Void> readyFuture) {
    if (readyFuture.isDone()) {
      return;
    }
    final ConnectivityState current;
    try {
      current = channel.getState(false);
    } catch (UnsupportedOperationException ignored) {
      readyFuture.set(null);
      return;
    }
    if (current == null || current == ConnectivityState.READY) {
      readyFuture.set(null);
      return;
    }
    if (current == ConnectivityState.TRANSIENT_FAILURE) {
      readyFuture.setException(
          Status.UNAVAILABLE
              .withDescription("Channel entered TRANSIENT_FAILURE during warm-up")
              .asRuntimeException());
      return;
    }
    if (current == ConnectivityState.SHUTDOWN) {
      readyFuture.setException(
          Status.UNAVAILABLE
              .withDescription("Channel shut down during warm-up")
              .asRuntimeException());
      return;
    }
    if (current == ConnectivityState.IDLE) {
      requestConnection(channel);
    }
    observeStateChange(channel, current, readyFuture);
  }

  private ApiFuture<Void> ensureCredentialPrefetchStarted() {
    if (activeCredentials == null || !activeCredentials.hasRequestMetadata()) {
      return ApiFutures.immediateFuture(null);
    }
    SettableApiFuture<Void> newFuture = null;
    synchronized (credentialLock) {
      if (credentialPrefetchFuture != null) {
        if (!credentialPrefetchFuture.isDone() || succeeded(credentialPrefetchFuture)) {
          return credentialPrefetchFuture;
        }
      }
      newFuture = SettableApiFuture.create();
      credentialPrefetchFuture = newFuture;
    }
    prefetchCredentialsInto(activeCredentials, metadataUri, executor, newFuture);
    return newFuture;
  }

  private static boolean succeeded(ApiFuture<Void> future) {
    if (future.isCancelled()) {
      return false;
    }
    try {
      future.get();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @VisibleForTesting
  static void prefetchCredentialsInto(
      Credentials credentials,
      @Nullable URI uri,
      Executor executor,
      SettableApiFuture<Void> targetFuture) {
    try {
      credentials.getRequestMetadata(
          uri,
          executor,
          new RequestMetadataCallback() {
            @Override
            public void onSuccess(Map<String, List<String>> metadata) {
              targetFuture.set(null);
            }

            @Override
            public void onFailure(Throwable exception) {
              targetFuture.setException(exception);
            }
          });
    } catch (RejectedExecutionException e) {
      targetFuture.setException(
          Status.UNAVAILABLE
              .withDescription("Background executor shut down during credential warm-up")
              .withCause(e)
              .asRuntimeException());
    } catch (RuntimeException e) {
      targetFuture.setException(e);
    }
  }
}
