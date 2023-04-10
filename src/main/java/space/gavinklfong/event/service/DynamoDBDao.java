package space.gavinklfong.event.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;
import space.gavinklfong.event.model.ShowItem;
import space.gavinklfong.event.model.TicketItem;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import space.gavinklfong.event.model.TicketStatus;

import java.time.LocalDateTime;
import java.util.*;

@Slf4j
@Service
public class DynamoDBDao {

    private static final String TABLE_NAME = "theatre-show-ticket";

    private static final String SHOW_ITEM_SORT_KEY = "SHOW";

    private final DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .region(Region.US_EAST_2)
            .credentialsProvider(ProfileCredentialsProvider.create())
            .build();

    private final DynamoDbEnhancedClient dynamoDbEnhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(dynamoDbClient)
            .build();

    public void saveShow(ShowItem showItem) {
        DynamoDbTable<ShowItem> table = dynamoDbEnhancedClient.table(TABLE_NAME,
                TableSchema.fromBean(ShowItem.class));

        PutItemEnhancedRequest<ShowItem> request = PutItemEnhancedRequest.builder(ShowItem.class)
                .item(showItem)
                .build();

        table.putItem(request);
    }

    public void saveTicket(TicketItem ticketItem) {
        DynamoDbTable<TicketItem> table = dynamoDbEnhancedClient.table(TABLE_NAME,
                TableSchema.fromBean(TicketItem.class));

        PutItemEnhancedRequest<TicketItem> request = PutItemEnhancedRequest.builder(TicketItem.class)
                .item(ticketItem)
                .build();

        table.putItem(request);
    }

    public void deleteTable() {

        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(TABLE_NAME)
                .build();

        dynamoDbClient.deleteTable(request);
    }

    public void createTable() {
        DynamoDbWaiter dbWaiter = dynamoDbClient.waiter();
        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(buildAttributeDefinitions())
                .keySchema(KeySchemaElement.builder()
                        .attributeName("showId")
                        .keyType(KeyType.HASH)
                        .build(),
                        KeySchemaElement.builder()
                        .attributeName("sortKey")
                        .keyType(KeyType.RANGE)
                        .build())
                .localSecondaryIndexes(buildLocalSecondaryIndexes())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(Long.valueOf(1))
                        .writeCapacityUnits(Long.valueOf(1))
                        .build())
                .tableName(TABLE_NAME)
                .build();

        dynamoDbClient.createTable(request);

        DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                .tableName(TABLE_NAME)
                .build();

        // Wait until the Amazon DynamoDB table is created
        WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest);
        waiterResponse.matched().response().ifPresent(System.out::println);

    }

    private List<LocalSecondaryIndex> buildLocalSecondaryIndexes() {
        return List.of(LocalSecondaryIndex.builder()
                        .keySchema(
                                KeySchemaElement.builder()
                                        .attributeName("showId")
                                        .keyType(KeyType.HASH)
                                        .build(),
                                KeySchemaElement.builder()
                                        .attributeName("status")
                                        .keyType(KeyType.RANGE)
                                        .build())
                        .indexName("ticket-status-index")
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .build(),
                LocalSecondaryIndex.builder()
                        .keySchema(
                                KeySchemaElement.builder()
                                        .attributeName("showId")
                                        .keyType(KeyType.HASH)
                                        .build(),
                                KeySchemaElement.builder()
                                        .attributeName("ticketRef")
                                        .keyType(KeyType.RANGE)
                                        .build())
                        .indexName("ticket-ref-index")
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .build());
    }

    private List<AttributeDefinition> buildAttributeDefinitions() {
        return List.of(
                AttributeDefinition.builder()
                .attributeName("status")
                .attributeType(ScalarAttributeType.S)
                .build(),
                AttributeDefinition.builder()
                        .attributeName("ticketRef")
                        .attributeType(ScalarAttributeType.S)
                        .build(),
                AttributeDefinition.builder()
                        .attributeName("showId")
                        .attributeType(ScalarAttributeType.S)
                        .build(),
                AttributeDefinition.builder()
                        .attributeName("sortKey")
                        .attributeType(ScalarAttributeType.S)
                        .build());
    }

    public ImmutablePair<ShowItem, List<TicketItem>> retrieveShowTickets(String showId) {

        Map<String, AttributeValue> attrValues = new HashMap<>();

        attrValues.put(":showId", AttributeValue.builder()
                .s(showId)
                .build());

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("showId = :showId")
                .expressionAttributeValues(attrValues)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);

        ShowItem showItem = ShowItem.builder().build();
        List<TicketItem> ticketItems = new ArrayList<>();

        List<Map<String, AttributeValue>> items = response.items();
        for (Map<String, AttributeValue> item : items) {
            if (SHOW_ITEM_SORT_KEY.equals(item.get("sortKey").s())) {
                showItem = DynamoItemMapper.mapShowItem(item);
            } else {
                ticketItems.add(DynamoItemMapper.mapTicketItem(item));
            }
        }

        return new ImmutablePair<>(showItem, ticketItems);
    }
}
