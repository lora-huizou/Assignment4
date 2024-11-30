package db;

import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

public class DynamoDBConnectionTest {
  public static void main(String[] args) {
    // Create DynamoDB client - no need for credentials when using LabRole
    DynamoDbClient ddb = DynamoDbClient.builder()
        .region(Region.US_WEST_2)
        .build();

    try {
      // Test1:  listing tables
      ListTablesResponse response = ddb.listTables();
      System.out.println("Connected successfully!");
      System.out.println("Available tables: " + response.tableNames());

      // Test2:  accessing your table
      DescribeTableRequest request = DescribeTableRequest.builder()
          .tableName("SkiResortData")
          .build();

      DescribeTableResponse tableResponse = ddb.describeTable(request);
      System.out.println("Table status: " + tableResponse.table().tableStatus());

      // Test 3: Try a Simple Write
      System.out.println("\nTest 3: Testing write operation...");
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("skierID", AttributeValue.builder().n("1").build());
      item.put("resortID#seasonID#dayID#time", AttributeValue.builder().s("1#2024#1#100").build());
      item.put("resortId", AttributeValue.builder().n("1").build());
      item.put("liftId", AttributeValue.builder().n("1").build());
      item.put("vertical", AttributeValue.builder().n("10").build());


      PutItemRequest putRequest = PutItemRequest.builder()
          .tableName("SkiResortData")
          .item(item)
          .build();

      ddb.putItem(putRequest);
      System.out.println("Successfully wrote test item");

      // Test 4: Try a Simple Read
      System.out.println("\nTest 4: Testing read operation...");
      GetItemRequest getRequest = GetItemRequest.builder()
          .tableName("SkiResortData")
          .key(Map.of(
              "skierID", AttributeValue.builder().n("1").build(),
              "resortID#seasonID#dayID#time", AttributeValue.builder().s("1#2024#1#100").build()
          ))
          .build();

      GetItemResponse getResponse = ddb.getItem(getRequest);
      if (getResponse.hasItem()) {
        System.out.println("Successfully retrieved test item");
        System.out.println("Item: " + getResponse.item());
      }
      System.out.println("\nAll tests completed successfully!");

    } catch (DynamoDbException e) {
      System.err.println("Error connecting to DynamoDB: " + e.getMessage());
      e.printStackTrace();
    }
  }
}