package db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import model.LiftRideEvent;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@Slf4j
public class SkiResortDao2 {
  private final DynamoDbClient ddb;
  private static final String TABLE_NAME = "SkiResortData";
  private static final String GSI_NAME_SKIER_INDEX = "SkierIndex"; // pk: skierId, sk:seasonID#dayID#time
  private static final String GSI_NAME_RESORT_SKIER_INDEX = "ResortSkierIndex"; //pk:resortID#seasonID#dayID, sk:skierId
  private static final String GSI_NAME_VERTICAL_INDEX = "SkierVerticalIndex"; //pk: skierId, sk:resortID#seasonID#dayID
  private static final int MAX_RETRIES = 5;


  public SkiResortDao2() {
    this.ddb = DynamoDbClient.builder()
        .region(Region.US_WEST_2)
        .credentialsProvider(InstanceProfileCredentialsProvider.create())
        .build();
  }

  public void addLiftRidesBatch(List<LiftRideEvent> events) {
    int maxBatchSize = 25; // DynamoDB allows a maximum of 25 items per batch
    List<WriteRequest> currentBatch = new ArrayList<>();

    for (LiftRideEvent event : events) {
      Map<String, AttributeValue> item = createItem(event);
      currentBatch.add(
          WriteRequest.builder()
              .putRequest(PutRequest.builder().item(item).build())
              .build()
      );

      // If the current batch reaches the maximum size, process it
      if (currentBatch.size() == maxBatchSize) {
        processBatch(currentBatch);
        currentBatch.clear(); // Clear the batch for the next set of items
      }
    }

    // Process any remaining items in the current batch
    if (!currentBatch.isEmpty()) {
      processBatch(currentBatch);
    }
  }

  private void processBatch(List<WriteRequest> batch) {
    int retryCount = 0;
    boolean success = false;

    while (!success && retryCount < MAX_RETRIES) {
      try {
        // Batch write request
        BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
            .requestItems(Map.of(TABLE_NAME, batch))
            .build();

        // Execute the batch write
        BatchWriteItemResponse response = ddb.batchWriteItem(batchRequest);

        // Check for unprocessed items and retry
        Map<String, List<WriteRequest>> unprocessedItems = response.unprocessedItems();
        if (unprocessedItems.isEmpty()) {
          success = true;
        } else {
          // Retry unprocessed items
          batch = unprocessedItems.get(TABLE_NAME);
          retryCount++;
          log.warn("Retrying {} unprocessed items (retry count: {})", batch.size(), retryCount);
          Thread.sleep(100 * retryCount); // Exponential backoff
        }
      } catch (DynamoDbException | InterruptedException e) {
        retryCount++;
        log.error("Error during batch write: ", e);
        try {
          Thread.sleep(100 * retryCount); // Exponential backoff
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    if (!success) {
      log.error("Batch write failed after {} retries.", retryCount);
    }
  }

  public void addLiftRide(LiftRideEvent event) {
    List<LiftRideEvent> batch = new ArrayList<>();
    batch.add(event);
    // Call the batch write method
    addLiftRidesBatch(batch);
  }

  private Map<String, AttributeValue> createItem(LiftRideEvent event) {
    // pk: resortID#seasonID#dayID
    // sk: skierID#time
    String pk = String.format("%d#%s#%d", event.getResortID(), event.getSeasonID(), event.getDayID());
    String sk = String.format("%d#%d", event.getSkierID(), event.getLiftRide().getTime());
    int vertical = event.getLiftRide().getLiftID() * 10;

    Map<String, AttributeValue> item = new HashMap<>();
    // Base table keys
    item.put("PK", AttributeValue.builder().s(pk).build());
    item.put("SK", AttributeValue.builder().s(sk).build());
    // Attributes
    item.put("skierID", AttributeValue.builder()
        .n(String.valueOf(event.getSkierID())).build());
    item.put("vertical", AttributeValue.builder()
        .n(String.valueOf(vertical)).build());
    item.put("resortID", AttributeValue.builder()
        .n(String.valueOf(event.getResortID())).build());
    item.put("seasonID", AttributeValue.builder()
        .s(event.getSeasonID()).build());
    item.put("dayID", AttributeValue.builder()
        .n(String.valueOf(event.getDayID())).build());
    item.put("time", AttributeValue.builder()
        .n(String.valueOf(event.getLiftRide().getTime())).build());

    // TODO: Add GSI keys
    return item;
  }

  // Query 1: Get unique skiers for a resort/season/day
  public long getUniqueSkiersForResort(int resortID, String seasonID, int dayID) {
    String pk = String.format("%d#%s#%d", resortID, seasonID, dayID);

    QueryRequest request = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("PK = :pk")
        .expressionAttributeValues(Map.of(":pk", AttributeValue.builder().s(pk).build()))
        .projectionExpression("skierID")
        .build();

    try {
      QueryResponse response = ddb.query(request);
      return response.items().stream()
          .map(item -> item.get("skierID").n()) // Extract skierID
          .distinct() // Deduplicate skierIDs
          .count();
    } catch (DynamoDbException e) {
      log.error("Error getting unique skiers: ", e);
      throw e;
    }
  }

  // Query 2: Get total vertical for a skier on a specific day
  public int getTotalVerticalForSkierDay(String skierID, String seasonID, int dayID, int resortID) {
    String pk = String.format("%d#%s#%d", resortID, seasonID, dayID);

    QueryRequest request = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("PK = :pk and begins_with(SK, :skierPrefix)")
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(pk).build(),
            ":skierPrefix", AttributeValue.builder().s(skierID + "#").build()))
        .projectionExpression("vertical")
        .build();

    try {
      QueryResponse response = ddb.query(request);
      return response.items().stream()
          .mapToInt(item -> Integer.parseInt(item.get("vertical").n()))
          .sum();
    } catch (DynamoDbException e) {
      log.error("Error getting total vertical for skier day: ", e);
      throw e;
    }
  }

  // Query 3: Get total vertical for a skier across all seasons
  public int getTotalVerticalForSkier(String skierID) {
    QueryRequest request = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME_SKIER_INDEX)
        .keyConditionExpression("skierID = :skierID")
        .expressionAttributeValues(Map.of(":skierID", AttributeValue.builder().s(skierID).build()))
        .projectionExpression("vertical")
        .build();

    try {
      QueryResponse response = ddb.query(request);
      return response.items().stream()
          .mapToInt(item -> Integer.parseInt(item.get("vertical").n()))
          .sum();
    } catch (DynamoDbException e) {
      log.error("Error getting total vertical for skier: ", e);
      throw e;
    }
  }

  // Query 4: Get vertical totals for a skier by season/day
  public Map<String, Integer> getVerticalBySeasonAndDay(String skierID) {
    QueryRequest request = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME_VERTICAL_INDEX)
        .keyConditionExpression("skierID = :skierID")
        .expressionAttributeValues(Map.of(":skierID", AttributeValue.builder().s(skierID).build()))
        .projectionExpression("SK, vertical")
        .build();

    try {
      QueryResponse response = ddb.query(request);
      Map<String, Integer> verticalByDay = new HashMap<>();

      response.items().forEach(item -> {
        String key = item.get("SK").s();
        int vertical = Integer.parseInt(item.get("vertical").n());
        verticalByDay.merge(key, vertical, Integer::sum);
      });

      return verticalByDay;
    } catch (DynamoDbException e) {
      log.error("Error getting vertical by season and day: ", e);
      throw e;
    }
  }




}

