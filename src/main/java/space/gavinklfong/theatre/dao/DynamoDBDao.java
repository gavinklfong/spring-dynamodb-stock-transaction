package space.gavinklfong.theatre.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import space.gavinklfong.theatre.exception.TicketReservationException;
import space.gavinklfong.theatre.model.SeatArea;
import space.gavinklfong.theatre.model.ShowItem;
import space.gavinklfong.theatre.model.TicketItem;
import space.gavinklfong.theatre.model.TicketStatus;

import java.util.*;

import static java.util.stream.Collectors.averagingDouble;
import static java.util.stream.Collectors.groupingBy;
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

    public void deleteTicket(TicketItem ticketItem) {
        Map<String, AttributeValue> attrValues = Map.of(
                ":showId", AttributeValue.builder().s(ticketItem.getShowId()).build(),
                ":ticketId", AttributeValue.builder().s(ticketItem.getSortKey()).build()
        );

        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
                .tableName(TABLE_NAME)
                .key(attrValues)
                .build();

        dynamoDbClient.deleteItem(deleteItemRequest);
    }

    public void reserveTicket(String showId, String ticketId, String ticketRef) {

        UpdateItemRequest updateRequest = createReserveTicketRequest(showId, ticketId, ticketRef);

        try {
            dynamoDbClient.updateItem(updateRequest);
        } catch (ConditionalCheckFailedException e) {
            throw new TicketReservationException();
        }

    }

    private UpdateItemRequest createReserveTicketRequest(String showId, String ticketId, String ticketRef) {
        return UpdateItemRequest.builder()
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

    }

    public void reserveTickets(String showId, Set<String> ticketIds, String ticketRef) {

        List<TransactWriteItem> actions = ticketIds.stream()
                .map(ticketId -> createTicketReservationUpdateRequest(showId, ticketId, ticketRef))
                .map(this::wrapInTransactWriteItem)
                .toList();

        TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
                .transactItems(actions)
                .build();

        dynamoDbClient.transactWriteItems(request);
    }

    private Update createTicketReservationUpdateRequest(String showId, String ticketId, String ticketRef) {
        return Update.builder()
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
    }

    private TransactWriteItem wrapInTransactWriteItem(Update update) {
        return TransactWriteItem.builder()
                .update(update)
                .build();
    }

    public Map<SeatArea, Double> findAverageTicketPriceBySeatArea(String showId) {
        Map<String, AttributeValue> attrValues = Map.of(
                ":showId", AttributeValue.builder().s(showId).build(),
                ":ticketSortKeyPrefix", AttributeValue.builder().s(TICKET_ITEM_SORT_KEY_PREFIX).build()
        );

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("showId = :showId AND begins_with(sortKey, :ticketSortKeyPrefix)")
                .expressionAttributeValues(attrValues)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);

        return response.items().stream()
                .map(DynamoItemMapper::mapTicketItem)
                .collect(groupingBy(TicketItem::getArea, averagingDouble(TicketItem::getPrice)));
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
            String sortKey = item.get("sortKey").s();
            if (SHOW_ITEM_SORT_KEY.equals(sortKey)) {
                showItem = DynamoItemMapper.mapShowItem(item);
            } else if (sortKey.startsWith(TICKET_ITEM_SORT_KEY_PREFIX)) {
                ticketItems.add(DynamoItemMapper.mapTicketItem(item));
            }
        }

        return new ImmutablePair<>(showItem, ticketItems);
    }
}
