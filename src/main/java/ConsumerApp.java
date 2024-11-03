import com.rabbitmq.client.ConnectionFactory;

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
  private static final int NUM_WORKERS = 100; // number of consumer threads
  private static final String SERVER = "34.213.239.60"; // rabbitmq elastic ip
  //private static final String SERVER = "localhost";
  private static final String QUEUE_NAME = "LiftRideQueue";

  public static void main(String[] args) {
    LiftRideStorage storage = new LiftRideStorage();
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(SERVER);
    factory.setPort(5672);
    factory.setUsername("guest");
    factory.setPassword("guest");

    ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);
    List<ConsumerWorker> workers = new ArrayList<>();
    // Start worker threads in the ExecutorService
    for (int i = 0; i < NUM_WORKERS; i++) {
      ConsumerWorker worker = new ConsumerWorker(QUEUE_NAME, storage, factory);
      workers.add(worker);
      executor.submit(worker);
    }
    // Add shutdown hook, ensure that all consumers complete their work and that resources are closed.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Shutdown hook triggered. Shutting down executor and consumer workers.");
      executor.shutdown();
      // first shut down ExecutorService
      try {
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          log.info("Executor did not terminate in set time. Forcing shutdown.");
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow(); // Re-interrupt if current thread was interrupted
        Thread.currentThread().interrupt();
      }

      // then close each worker's resources (channel & connection)
      for (ConsumerWorker worker : workers) {
        worker.close();
      }
    }));

  }
}

