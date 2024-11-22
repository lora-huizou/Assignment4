import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.rabbitmq.client.*;


import db.PerformanceMonitor;
import db.SkiResortDao;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import model.LiftRideEvent;

/**
 * Handle message consumption, each instance do the following
 * 1. connect to rabbitmq queue
 * 2. consume message
 * 3. parse and store lift ride data to LiftRideStorage
 */
@Slf4j
public class ConsumerWorker implements Runnable{
  private final String queueName;
  private final ConnectionFactory factory;
  private Connection connection;
  private Channel channel;
  private final SkiResortDao skiResortDao;
  private final PerformanceMonitor monitor;

  public ConsumerWorker(String queueName, SkiResortDao skiResortDao, ConnectionFactory factory) {
    this.queueName = queueName;
    this.skiResortDao = skiResortDao;
    this.factory = factory;
    this.monitor = PerformanceMonitor.getInstance(); //Singleton pattern
    initializeConnectionAndChannel();
  }

  private void initializeConnectionAndChannel() {
    try {
      connection = factory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare(queueName, true, false, false, null);
      channel.basicQos(100); // pre fetch
      //log.info("Connected to RabbitMQ and created channel.");
    } catch (IOException | TimeoutException e) {
      log.error("Failed to create connection or channel: {}", e.getMessage());
    }
  }

  @Override
  public void run() {
    try {
      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        long startTime = System.currentTimeMillis();
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        //log.info("Message received: {}" , message);
        try {
          // Update queue backlog
          try {
            AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(queueName);
            monitor.updateQueueBacklog(response.getMessageCount());
          } catch (IOException e) {
            log.warn("Failed to get queue count: {}", e.getMessage());
          }
          // process message
          LiftRideEvent liftRideMessage = parseLiftRide(message);
          //log.info("lift ride Message received: {}" , liftRideMessage);
          skiResortDao.addLiftRide(liftRideMessage);
          if (channel != null && channel.isOpen()) {
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            //log.info("Message processed and acknowledged.");
            long latency = System.currentTimeMillis() - startTime;
            monitor.recordWrite(true, latency);
          }
        } catch (Exception e) {
          long latency = System.currentTimeMillis() - startTime;
          monitor.recordWrite(false, latency);
          log.error("Failed to process message: {}", e.getMessage(), e);
          e.printStackTrace();
          if (channel != null && channel.isOpen()) {
            // If it's a circuit breaker or DynamoDB throttling exception,
            // add delay before requeue
            if (isThrottlingError(e)) {
              try {
                Thread.sleep(1000); // Add delay before requeue
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              }
            }
            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true); // Requeue the message
          }
        }
      };

      if (channel != null && channel.isOpen()) {
        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
          log.info("Consumer cancelled: {}", consumerTag);
        });
      }
    } catch (IOException e) {
      e.printStackTrace();
      log.error("Failed to connect to RabbitMQ or consume messages: {}", e.getMessage());
    }
  }

  private boolean isThrottlingError(Exception e) {
    String message = e.getMessage().toLowerCase();
    return message.contains("circuit breaker") ||
        message.contains("throttling") ||
        message.contains("provisioned throughput exceeded");
  }

  private LiftRideEvent parseLiftRide(String message) {
    try {
      LiftRideEvent liftRideMessage = new Gson().fromJson(message, LiftRideEvent.class);
      //log.info("Parsed lift ride message is: {}", liftRideMessage);
      return liftRideMessage;
    } catch (JsonSyntaxException e){
      //log.error("Json parsing failed: {}", e.getMessage());
      throw e;
    }
  }

  public void close() {
    try {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
      if (connection != null && connection.isOpen()) {
        connection.close();
      }
    } catch (IOException | TimeoutException e) {
      log.error("Error closing connection or channel: {}", e.getMessage());
    }
  }

}
