package space.gavinklfong.theatre.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import space.gavinklfong.theatre.model.ShowItem;
import space.gavinklfong.theatre.model.TicketItem;
import space.gavinklfong.theatre.model.TicketStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.stringValue;
import static space.gavinklfong.theatre.dao.DynamoDBTableConstant.*;

@Slf4j
@RequiredArgsConstructor
@Service
public class DynamoDBDao {

    private final DynamoDbClient dynamoDbClient;

    public void saveShow(ShowItem showItem) {
        PutItemRequest putRequest = PutItemRequest.builder()
                .item(showItem.toAttributeValues())
                .tableName(TABLE_NAME)
                .build();

        dynamoDbClient.putItem(putRequest);
    }

    public void saveTicket(TicketItem ticketItem) {
        PutItemRequest putRequest = PutItemRequest.builder()
                .item(ticketItem.toAttributeValues())
                .tableName(TABLE_NAME)
                .build();

        dynamoDbClient.putItem(putRequest);
    }

    public void reserveTicket(String showId, String ticketId, String ticketRef) {

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .conditionExpression("#status = :expected_status")
                .updateExpression("SET #status = :new_status, #ticketRef = :new_reference")
                .expressionAttributeNames(Map.of(
                        "#status", "status",
                        "#ticketRef", "ticketRef"))
                .expressionAttributeValues(Map.of(
                        ":expected_status", stringValue(TicketStatus.AVAILABLE.name()),
                        ":new_status", stringValue(TicketStatus.RESERVED.name()),
                        ":new_reference", stringValue(ticketRef)
                        ))
                .key(Map.of("showId", stringValue(showId),
                        "sortKey", stringValue(ticketId)))
                .tableName(TABLE_NAME)
                .build();

        dynamoDbClient.updateItem(updateRequest);
    }

    public Optional<TicketItem> findTicketById(String showId, String ticketId) {
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
                .map(DynamoItemMapper::mapTicketItem);
    }

    public Optional<TicketItem> findTicketByReference(String showId, String ticketRef) {
        Map<String, AttributeValue> attrValues = Map.of(
                ":showId", AttributeValue.builder().s(showId).build(),
                ":ticketRef", AttributeValue.builder().s(ticketRef).build()
        );

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("showId = :showId AND ticketRef = :ticketRef")
                .expressionAttributeValues(attrValues)
                .indexName(TICKET_REF_INDEX)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);

        return response.items().stream()
                .findFirst()
                .map(DynamoItemMapper::mapTicketItem);
    }

    public Optional<ShowItem> findShowById(String showId) {

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
                .map(DynamoItemMapper::mapShowItem);
    }

    public ImmutablePair<ShowItem, List<TicketItem>> findShowAndTicketsById(String showId) {

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
