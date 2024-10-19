package space.gavinklfong.stock.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import space.gavinklfong.stock.exception.TicketReservationException;
import space.gavinklfong.stock.model.*;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.util.stream.Collectors.averagingDouble;
import static java.util.stream.Collectors.groupingBy;
import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.stringValue;
import static space.gavinklfong.stock.dao.DynamoDBTableConstant.*;

@Slf4j
@RequiredArgsConstructor
@Service
public class DynamoDBDao {

    private final DynamoDbClient dynamoDbClient;

    public void saveStockTransaction(StockTransaction transaction) {
        PutItemRequest putRequest = PutItemRequest.builder()
                .item(transaction.toAttributeValues())
                .tableName(TABLE_NAME)
                .build();

        dynamoDbClient.putItem(putRequest);
    }

    public List<StockTransaction> findStockTransactionByAccountNumber(String accountNumber) {
        Map<String, AttributeValue> attrValues = Map.of(
                ":accountNumber", AttributeValue.builder().s(accountNumber).build()
        );

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("accountNumber = :accountNumber")
                .expressionAttributeValues(attrValues)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);

        return response.items().stream()
                .map(StockTransaction::toStockTransaction)
                .toList();
    }

    public List<StockTransaction> findStockTransactionByAccountNumberWithTimeRange(String accountNumber,
                                                                                   Instant startTime,
                                                                                   Instant endTime) {
        Map<String, AttributeValue> attrValues = Map.of(
                ":accountNumber", AttributeValue.builder().s(accountNumber).build(),
                ":startTime", AttributeValue.builder().s(DateTimeFormatter.ISO_INSTANT.format(startTime)).build(),
                ":endTime", AttributeValue.builder().s(DateTimeFormatter.ISO_INSTANT.format(endTime)).build()
        );

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("accountNumber = :accountNumber AND timestamp BETWEEN :startTime AND :endTime")
                .expressionAttributeValues(attrValues)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);

        return response.items().stream()
                .map(StockTransaction::toStockTransaction)
                .toList();
    }
}
