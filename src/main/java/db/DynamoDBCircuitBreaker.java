package db;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamoDBCircuitBreaker {
  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final AtomicLong lastFailureTime = new AtomicLong(0);
  private final AtomicBoolean isOpen = new AtomicBoolean(false);

  private static final int FAILURE_THRESHOLD = 10;
  private static final long RESET_TIMEOUT_MS = 5000; // 5 seconds
  private static final long BASE_BACKOFF_MS = 100;
  private static final int MAX_RETRIES = 3;

  public void recordFailure() {
    failureCount.incrementAndGet();
    lastFailureTime.set(System.currentTimeMillis());

    if (failureCount.get() >= FAILURE_THRESHOLD) {
      isOpen.set(true);
      log.warn("Circuit breaker opened due to {} failures", FAILURE_THRESHOLD);
    }
  }

  public void recordSuccess() {
    failureCount.set(0);
    if (isOpen.get()) {
      isOpen.set(false);
      log.info("Circuit breaker closed after successful operation");
    }
  }

  public boolean shouldAllowRequest() {
    if (!isOpen.get()) {
      return true;
    }

    long timeSinceLastFailure = System.currentTimeMillis() - lastFailureTime.get();
    if (timeSinceLastFailure >= RESET_TIMEOUT_MS) {
      isOpen.set(false);
      failureCount.set(0);
      return true;
    }

    return false;
  }
  public void waitWithBackoff(int retryCount) throws InterruptedException {
    if (retryCount >= MAX_RETRIES) {
      throw new RuntimeException("Max retries exceeded");
    }

    long backoffTime = BASE_BACKOFF_MS * (long) Math.pow(2, retryCount);
    Thread.sleep(backoffTime);
  }
}
