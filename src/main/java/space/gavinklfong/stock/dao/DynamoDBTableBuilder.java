package space.gavinklfong.stock.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.util.List;

import static space.gavinklfong.stock.dao.DynamoDBTableConstant.TABLE_NAME;


@Slf4j
@RequiredArgsConstructor
public class DynamoDBTableBuilder {

    private final DynamoDbClient dynamoDbClient;

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
                .keySchema(buildKeySchemaElements())
                .localSecondaryIndexes(buildLocalSecondaryIndexes())
                .provisionedThroughput(
                        ProvisionedThroughput.builder()
                        .readCapacityUnits(1L)
                        .writeCapacityUnits(1L)
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

    private List<KeySchemaElement> buildKeySchemaElements() {
        return List.of(KeySchemaElement.builder()
                        .attributeName("showId")
                        .keyType(KeyType.HASH)
                        .build(),
                KeySchemaElement.builder()
                        .attributeName("sortKey")
                        .keyType(KeyType.RANGE)
                        .build());
    }

    private List<LocalSecondaryIndex> buildLocalSecondaryIndexes() {
        return List.of(LocalSecondaryIndex.builder()
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

}
