import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.rabbitmq.client.*;


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
  private final LiftRideStorage storage;
  private final ConnectionFactory factory;
  private Connection connection;
  private Channel channel;

  public ConsumerWorker(String queueName, LiftRideStorage storage, ConnectionFactory factory) {
    this.queueName = queueName;
    this.storage = storage;
    this.factory = factory;
    initializeConnectionAndChannel();
  }

  private void initializeConnectionAndChannel() {
    try {
      connection = factory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare(queueName, true, false, false, null);
      channel.basicQos(50); // pre fetch
      //log.info("Connected to RabbitMQ and created channel.");
    } catch (IOException | TimeoutException e) {
      log.error("Failed to create connection or channel: {}", e.getMessage());
    }
  }

  @Override
  public void run() {
    try {
      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        //log.info("Message received: {}" , message);
        try {
          LiftRideEvent liftRideMessage = parseLiftRide(message);
          //log.info("lift ride Message received: {}" , liftRideMessage);
          storage.addLiftRide(liftRideMessage.getSkierID(), liftRideMessage.getLiftRide());
          if (channel != null && channel.isOpen()) {
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            //log.info("Message processed and acknowledged.");
          }
        } catch (Exception e) {
          log.error("Failed to process message: {}", e.getMessage(), e);
          e.printStackTrace();
          if (channel != null && channel.isOpen()) {
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
