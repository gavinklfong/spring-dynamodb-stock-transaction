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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import space.gavinklfong.event.model.ShowItem;
import space.gavinklfong.event.model.TicketItem;
import space.gavinklfong.event.model.TicketStatus;

import java.time.LocalDateTime;
import java.util.*;

@Slf4j
@Service
public class DynamoDBDao {

    private static final String TABLE_NAME = "theatre-show-ticket";

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

    public ImmutablePair<ShowItem, List<TicketItem>> retrieveShowTickets(String showId) {

        // Set up mapping of the partition name with the value.
        HashMap<String, AttributeValue> attrValues = new HashMap<>();

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
            if ("SHOW".equals(item.get("sortKey").s())) {
                showItem = DynamoItemMapper.mapShowItem(item);
            } else {
                ticketItems.add(DynamoItemMapper.mapTicketItem(item));
            }
        }

        return new ImmutablePair<>(showItem, ticketItems);
    }
}
