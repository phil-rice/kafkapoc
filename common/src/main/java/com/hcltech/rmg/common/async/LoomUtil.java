package com.hcltech.rmg.common.async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class LoomUtil {
  private LoomUtil() {}

  /** Returns a virtual-thread-per-task executor if running on JDK >= 21; otherwise null. */
  static ExecutorService tryNewVirtualThreadPerTaskExecutor() {
    try {
      // java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()
      var m = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
      return (ExecutorService) m.invoke(null);
    } catch (Throwable ignored) {
      return null; // Not available (e.g., JDK 17)
    }
  }
}
