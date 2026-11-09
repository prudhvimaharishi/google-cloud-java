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

import com.google.common.annotations.VisibleForTesting;
import java.util.function.Function;
import org.checkerframework.checker.nullness.qual.Nullable;

final class StorageWarmUpConfig {

  static final String SYS_PROP_WARMUP_ON_CREATION =
      "com.google.cloud.storage.grpc.warmup_on_creation";
  static final String ENV_ENABLE_WARMUP_ON_CREATION = "GCP_STORAGE_JAVA_ENABLE_WARMUP_ON_CREATION";

  private static volatile Function<String, String> sysPropResolver = System::getProperty;
  private static volatile Function<String, String> envResolver = System::getenv;

  private StorageWarmUpConfig() {}

  static boolean isWarmUpOnCreationEnabled(@Nullable Boolean builderSetting) {
    return isWarmUpOnCreationEnabled(sysPropResolver, envResolver, builderSetting);
  }

  @VisibleForTesting
  static boolean isWarmUpOnCreationEnabled(
      Function<String, String> sysProps,
      Function<String, String> env,
      @Nullable Boolean builderSetting) {
    if (builderSetting != null) {
      return builderSetting;
    }
    Boolean fromSysProp =
        StorageMetricsConfig.parseBooleanValue(
            sysProps.apply(SYS_PROP_WARMUP_ON_CREATION), SYS_PROP_WARMUP_ON_CREATION);
    if (fromSysProp != null) {
      return fromSysProp;
    }
    Boolean fromEnv =
        StorageMetricsConfig.parseBooleanValue(
            env.apply(ENV_ENABLE_WARMUP_ON_CREATION), ENV_ENABLE_WARMUP_ON_CREATION);
    if (fromEnv != null) {
      return fromEnv;
    }
    return false;
  }

  @VisibleForTesting
  static void setSysPropResolverForTesting(Function<String, String> testSysProps) {
    sysPropResolver = testSysProps != null ? testSysProps : System::getProperty;
  }

  @VisibleForTesting
  static void setEnvResolverForTesting(Function<String, String> testEnv) {
    envResolver = testEnv != null ? testEnv : System::getenv;
  }

  @VisibleForTesting
  static void resetResolversForTesting() {
    sysPropResolver = System::getProperty;
    envResolver = System::getenv;
  }
}
