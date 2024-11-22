package db;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PerformanceMonitor {
  private final AtomicInteger successfulWrites = new AtomicInteger(0);
  private final AtomicInteger failedWrites = new AtomicInteger(0);
  private final AtomicInteger queueBacklog = new AtomicInteger(0);
  private final AtomicLong totalSuccessLatency = new AtomicLong(0);
  private final AtomicLong totalFailureLatency = new AtomicLong(0);
  private final long startTime = System.currentTimeMillis();


  private static final PerformanceMonitor INSTANCE = new PerformanceMonitor();

  public static PerformanceMonitor getInstance() {
    return INSTANCE;
  }

  public void recordWrite(boolean success, long latencyMs) {
    if (success) {
      successfulWrites.incrementAndGet();
      totalSuccessLatency.addAndGet(latencyMs);
    } else {
      failedWrites.incrementAndGet();
      totalFailureLatency.addAndGet(latencyMs);
    }
  }

  public void updateQueueBacklog(int size) {
    queueBacklog.set(size);
  }

  public void printStats() {
    int totalSuccess = successfulWrites.get();
    int totalFailures = failedWrites.get();
    long currentTime = System.currentTimeMillis();
    double runtimeSeconds = (currentTime - startTime) / 1000.0;


    double avgSuccessLatency = totalSuccess > 0 ?
        (double) totalSuccessLatency.get() / totalSuccess : 0;
    double avgFailureLatency = totalFailures > 0 ?
        (double) totalFailureLatency.get() / totalFailures : 0;
    double throughput = runtimeSeconds > 0 ?
        (totalSuccess + totalFailures) / runtimeSeconds : 0;

    log.info("Performance Statistics:");
    log.info("Runtime: {} seconds", String.format("%.2f", runtimeSeconds));
    log.info("Successful writes: {}", totalSuccess);
    log.info("Failed writes: {}", totalFailures);
    log.info("Average success latency: {}ms", String.format("%.2f", avgSuccessLatency));
    log.info("Average failure latency: {}ms", String.format("%.2f", avgFailureLatency));
    log.info("Current queue backlog: {}", queueBacklog.get());
    log.info("Throughput: {} operations/second", String.format("%.2f", throughput));

    // Calculate success rate
    int totalOps = totalSuccess + totalFailures;
    if (totalOps > 0) {
      double successRate = (double) totalSuccess / totalOps * 100;
      log.info("Success rate: {}%", String.format("%.2f", successRate));
    }
  }

}
