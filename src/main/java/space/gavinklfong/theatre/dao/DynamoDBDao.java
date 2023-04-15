package space.gavinklfong.theatre.dao;

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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import space.gavinklfong.theatre.model.ShowItem;
import space.gavinklfong.theatre.model.TicketItem;
import space.gavinklfong.theatre.model.TicketStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.stringValue;
import static space.gavinklfong.theatre.dao.DynamoDBTableConstant.SHOW_ITEM_SORT_KEY;
import static space.gavinklfong.theatre.dao.DynamoDBTableConstant.TABLE_NAME;

@Slf4j
@Service
public class DynamoDBDao {

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

    public void reserveTicket(String showId, String ticketId) {

        UpdateItemRequest updateRequest1 = UpdateItemRequest.builder()
                .conditionExpression("#status = :expected_status")
                .updateExpression("SET #status = :new_status, #ticketRef = :new_reference")
                .expressionAttributeNames(Map.of(
                        "#status", "status",
                        "#ticketRef", "ticketRef"))
                .expressionAttributeValues(Map.of(
                        ":expected_status", stringValue(TicketStatus.AVAILABLE.name()),
                        ":new_status", stringValue(TicketStatus.RESERVED.name()),
                        ":new_reference", stringValue(UUID.randomUUID().toString())
                        ))
                .key(Map.of("showId", stringValue(showId),
                        "sortKey", stringValue(ticketId)))
                .tableName(TABLE_NAME)
                .build();

        dynamoDbClient.updateItem(updateRequest1);

//        DynamoDbTable<TicketItem> table = dynamoDbEnhancedClient.table(TABLE_NAME,
//                TableSchema.fromBean(TicketItem.class));
//
//        Key itemKey = Key.builder()
//                .partitionValue(showId)
//                .sortValue(ticketId)
//                .build();
//
//        Expression conditionExpression = Expression.builder()
//                .expression("#status = :expected_status")
//                .expressionValues(singletonMap(":expected_status", stringValue(TicketStatus.AVAILABLE.name())))
//                .expressionNames(singletonMap("#status", "status"))
//                .build();
//
//        ConditionCheck<TicketItem> conditionCheck = ConditionCheck.builder()
//                .key(itemKey)
//                .conditionExpression(conditionExpression)
//                .build();
//
//        TicketItem ticketItem = TicketItem.builder()
//                .showId(showId)
//                .sortKey(ticketId)
//                .status(TicketStatus.RESERVED)
//                .build();

//        UpdateItemEnhancedRequest<TicketItem> updateRequest0 = UpdateItemEnhancedRequest.builder(TicketItem.class)
//                .item(ticketItem)
//                .conditionExpression(conditionExpression)
//                .ignoreNulls(true)
//                .build();

//        TransactUpdateItemEnhancedRequest<TicketItem> updateRequest = TransactUpdateItemEnhancedRequest.builder(TicketItem.class)
//                .conditionExpression(conditionExpression)
//                .item(ticketItem)
//                .ignoreNulls(true)
//                .build();
//
//        TransactWriteItemsEnhancedRequest transactWriteItemsEnhancedRequest = TransactWriteItemsEnhancedRequest.builder()
//                .addUpdateItem(table, updateRequest)
//                .build();

//        dynamoDbEnhancedClient.transactWriteItems(transactWriteItemsEnhancedRequest);

    }

    public TicketItem retrieveTicket(String showId, String ticketId) {
        Map<String, AttributeValue> attrValues = Map.of(
                ":showId", AttributeValue.builder().s(showId).build(),
                ":ticketId", AttributeValue.builder().s(ticketId).build()
        );

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("showId = :showId AND sortKey = :ticketId")
                .expressionAttributeValues(attrValues)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);

        return response.items().stream()
                .findFirst()
                .map(DynamoItemMapper::mapTicketItem)
                .orElseThrow();
    }

    public ShowItem retrieveShowItem(String showId) {

        Map<String, AttributeValue> attrValues = Map.of(
                ":showId", AttributeValue.builder().s(showId).build(),
                ":showIdentifier", AttributeValue.builder().s(SHOW_ITEM_SORT_KEY).build()
        );

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("showId = :showId AND sortKey = :showIdentifier")
                .expressionAttributeValues(attrValues)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);

        return response.items().stream()
                .findFirst()
                .map(DynamoItemMapper::mapShowItem)
                .orElseThrow();
    }

    public ImmutablePair<ShowItem, List<TicketItem>> retrieveShowTickets(String showId) {

        Map<String, AttributeValue> attrValues = Map.of(
                ":showId", AttributeValue.builder().s(showId).build()
        );

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
