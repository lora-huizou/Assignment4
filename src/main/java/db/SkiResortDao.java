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
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@Slf4j
public class SkiResortDao {
  private final DynamoDbClient ddb;
  private final DynamoDBCircuitBreaker circuitBreaker;
  private final PerformanceMonitor monitor;
  private static final String TABLE_NAME = "SkiResortData";
  private static final String GSI_NAME = "ResortIndex";


  public SkiResortDao() {
    this.ddb = DynamoDbClient.builder()
        .region(Region.US_WEST_2)
        .credentialsProvider(InstanceProfileCredentialsProvider.create())
        .build();
    this.circuitBreaker = new DynamoDBCircuitBreaker();
    this.monitor = PerformanceMonitor.getInstance();

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

    while (!success && retryCount < 5) {
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
    //sort key: seasonID#dayID#time
    String sk = String.format("%s#%d#%d",
        event.getSeasonID(), event.getDayID(), event.getLiftRide().getTime());
    //resortID#seasonID#dayID
    String gsiPk = String.format("%d#%s#%d",
        event.getResortID(), event.getSeasonID(), event.getDayID());
    int vertical = event.getLiftRide().getLiftID() * 10;

    Map<String, AttributeValue> item = new HashMap<>();
    // Base table keys
    item.put("skierID", AttributeValue.builder() // Partition key
        .s(String.valueOf(event.getSkierID()))
        .build());
    item.put("seasonID#dayID#time", AttributeValue.builder() // Sort key
        .s(sk)
        .build());
    // GSI keys
    item.put("resortID#seasonID#dayID", AttributeValue.builder()
        .s(gsiPk)
        .build());
    item.put("skierID", AttributeValue.builder()
        .s(String.valueOf(event.getSkierID()))
        .build());

    // Attributes
    item.put("resortId", AttributeValue.builder()
        .n(String.valueOf(event.getResortID()))
        .build());
    item.put("liftId", AttributeValue.builder()
        .n(String.valueOf(event.getLiftRide().getLiftID()))
        .build());
    item.put("vertical", AttributeValue.builder()
        .n(String.valueOf(vertical))
        .build());
    return item;
  }

  // Required query methods
  // For skier N, how many days have they skied this season?
  public int getDaysSkiedInSeason(String skierId, String seasonId) {
    QueryRequest request = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("skierID = :skierID and begins_with(#sortKey, :seasonPrefix)")
        .expressionAttributeNames(Map.of(
            "#sortKey", "seasonID#dayID#time"
        ))
        .expressionAttributeValues(Map.of(
            ":skierID", AttributeValue.builder().s(skierId).build(),
            ":seasonPrefix", AttributeValue.builder().s(seasonId + "#").build()
        ))
        .build();

    try {
      QueryResponse response = ddb.query(request);
      return (int) response.items().stream()
          .map(item -> item.get("seasonID#dayID#time").s().split("#")[1]) // Get dayId
          .distinct()
          .count();
    } catch (DynamoDbException e) {
      log.error("Error getting days skied: ", e);
      throw e;
    }
  }

  // For skier N, what are the vertical totals for each ski day?
  public Map<String, Integer> getVerticalTotalsByDay(String skierId, String seasonId) {
    QueryRequest request = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("skierID = :skierID and begins_with(#sortKey, :seasonPrefix)")
        .expressionAttributeNames(Map.of(
            "#sortKey", "seasonID#dayID#time"
        ))
        .expressionAttributeValues(Map.of(
            ":skierID", AttributeValue.builder().s(skierId).build(),
            ":seasonPrefix", AttributeValue.builder().s(seasonId + "#").build()
        ))
        .build();

    try {
      QueryResponse response = ddb.query(request);
      Map<String, Integer> dailyTotals = new HashMap<>();

      response.items().forEach(item -> {
        String dayId = item.get("seasonID#dayID#time").s().split("#")[1];
        int vertical = Integer.parseInt(item.get("vertical").n());
        dailyTotals.merge(dayId, vertical, Integer::sum);
      });

      return dailyTotals;
    } catch (DynamoDbException e) {
      log.error("Error getting vertical totals: ", e);
      throw e;
    }
  }

  // For skier N, show me the lifts they rode on each ski day
  public List<Integer> getSkierDayLifts(String skierId, String seasonId, int dayId) {
    String skPrefix = String.format("%s#%d#", seasonId, dayId);

    QueryRequest request = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("skierID = :skierID and begins_with(#sortKey, :skPrefix)")
        .expressionAttributeNames(Map.of(
            "#sortKey", "seasonID#dayID#time"
        ))
        .expressionAttributeValues(Map.of(
            ":skierID", AttributeValue.builder().s(skierId).build(),
            ":skPrefix", AttributeValue.builder().s(skPrefix).build()
        ))
        .build();

    try {
      QueryResponse response = ddb.query(request);
      return response.items().stream()
          .map(item -> Integer.parseInt(item.get("liftId").n()))
          .collect(Collectors.toList());
    } catch (DynamoDbException e) {
      log.error("Error getting lifts: ", e);
      throw e;
    }
  }

  // How many unique skiers visited resort X on day N?
  public int getUniqueSkiersForResort(int resortId, String seasonId, int dayId) {
    String gsiKey = String.format("%d#%s#%d", resortId, seasonId, dayId);

    QueryRequest request = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("#gsiPk = :gsiKey")
        .expressionAttributeNames(Map.of(
            "#gsiPk", "resortID#seasonID#dayID"
        ))
        .expressionAttributeValues(Map.of(
            ":gsiKey", AttributeValue.builder().s(gsiKey).build()
        ))
        .select(Select.COUNT)
        .build();

    try {
      QueryResponse response = ddb.query(request);
      return response.count();
    } catch (DynamoDbException e) {
      log.error("Error getting unique skiers: ", e);
      throw e;
    }
  }
}

