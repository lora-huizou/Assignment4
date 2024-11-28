import com.rabbitmq.client.ConnectionFactory;

import db.PerformanceMonitor;
import dao.SkiResortDao;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * connect to rabbitmq queue
 * to start multiple consumer threads
 */
@Slf4j
public class ConsumerApp {
  private static final int NUM_WORKERS = 200; // number of consumer threads
  private static final String SERVER = "54.190.212.169"; // rabbitmq elastic ip
  //private static final String SERVER = "localhost";
  private static final int PORT_NUM = 5672;
  private static final String QUEUE_NAME = "LiftRideQueue";
  private static final String USER_NAME= "guest";
  private static final String PASSWORD = "guest";

  private static volatile boolean isShuttingDown = false;

  public static void main(String[] args) {
    SkiResortDao skiResortDao = new SkiResortDao();
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(SERVER);
    factory.setPort(PORT_NUM);
    factory.setUsername(USER_NAME);
    factory.setPassword(PASSWORD);

    ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);
    List<ConsumerWorker> workers = new ArrayList<>();
    // Start worker threads in the ExecutorService
    for (int i = 0; i < NUM_WORKERS; i++) {
      ConsumerWorker worker = new ConsumerWorker(QUEUE_NAME, skiResortDao, factory);
      workers.add(worker);
      executor.submit(worker);
    }
    // Add shutdown hook, ensure that all consumers complete their work and that resources are closed.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (!isShuttingDown) {  // Prevent duplicate shutdown
        isShuttingDown = true;
        log.info("Shutdown signal received. Starting cleanup...");
        shutdownExecutorAndWorkers(executor, workers, 5);
        log.info("Final Statistics:");
        PerformanceMonitor.getInstance().printStats();
      }
    }));
    try {
      while (!isShuttingDown) {
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      log.info("Main thread interrupted");
    }
  }

  private static void shutdownExecutorAndWorkers(ExecutorService executor,
      List<ConsumerWorker> workers, int timeoutMinutes) {
    try {
      log.info("Initiating shutdown with {} minute timeout...", timeoutMinutes);
      executor.shutdown();

      if (!executor.awaitTermination(timeoutMinutes, TimeUnit.MINUTES)) {
        log.warn("Executor did not terminate within {} minutes. Forcing shutdown.",
            timeoutMinutes);
        executor.shutdownNow();
      } else {
        log.info("Executor terminated successfully");
      }
    } catch (InterruptedException e) {
      log.error("Executor shutdown interrupted.", e);
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    // Close worker resources
    log.info("Closing {} worker resources...", workers.size());
    for (ConsumerWorker worker : workers) {
      worker.close();
    }
    log.info("All workers closed successfully");
  }
}

