package space.gavinklfong.stock.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import space.gavinklfong.stock.model.*;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.util.Objects.nonNull;
import static space.gavinklfong.stock.dao.DynamoDBTableConstant.*;

@Slf4j
@RequiredArgsConstructor
@Service
public class StockTransactionDao {

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

    public QueryResultPage<StockTransaction, StockTransactionKey> findStockTransactionByAccountNumber(String accountNumber, int pageSize,
                                                                                                      StockTransactionKey exclusiveStartKey) {
        Map<String, AttributeValue> attrValues = Map.of(
                ":accountNumber", AttributeValue.builder().s(accountNumber).build()
        );

        Map<String, AttributeValue> exclusiveStartKeyAttrValues = null;

        if (nonNull(exclusiveStartKey)) {
            exclusiveStartKeyAttrValues = Map.of(
                    "accountNumber", AttributeValue.builder().s(exclusiveStartKey.getAccountNumber()).build(),
                    "timestamp", AttributeValue.builder().s(exclusiveStartKey.getTimestampFormatted()).build()
            );
        }

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("accountNumber = :accountNumber")
                .expressionAttributeValues(attrValues)
                .exclusiveStartKey(exclusiveStartKeyAttrValues)
                .limit(pageSize)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);
        return QueryResultPage
                .<StockTransaction, StockTransactionKey>builder()
                .results(response.items().stream()
                        .map(StockTransaction::toStockTransaction)
                        .toList())
                .nextQueryKey(mapToStockTransactionKey(response))
                .build();
    }


    public QueryResultPage<StockTransaction, StockTransactionKey> findStockTransactionByAccountNumberAndTradeAction(
            String accountNumber, TradeAction tradeAction) {

        Map<String, AttributeValue> attrValues = Map.of(
                ":accountNumber", AttributeValue.builder().s(accountNumber).build(),
                ":tradeAction", AttributeValue.builder().s(tradeAction.name()).build()
        );

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("accountNumber = :accountNumber")
                .filterExpression("tradeAction = :tradeAction")
                .expressionAttributeValues(attrValues)
                .limit(10)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);
        return QueryResultPage
                .<StockTransaction, StockTransactionKey>builder()
                .results(response.items().stream()
                        .map(StockTransaction::toStockTransaction)
                        .toList())
                .nextQueryKey(mapToStockTransactionKey(response))
                .build();
    }

    private StockTransactionKey mapToStockTransactionKey(QueryResponse queryResponse) {
        return queryResponse.hasLastEvaluatedKey()
                ? StockTransactionKey.toStockTransactionKey(queryResponse.lastEvaluatedKey())
                : null;
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
                .keyConditionExpression("accountNumber = :accountNumber AND #timestamp BETWEEN :startTime AND :endTime")
                .expressionAttributeNames(Map.of("#timestamp", "timestamp"))
                .expressionAttributeValues(attrValues)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);

        return response.items().stream()
                .map(StockTransaction::toStockTransaction)
                .toList();
    }


    public QueryResultPage<StockTransaction, StockTransactionKey> findStockTransactionByAccountNumberAndTradeAction(String accountNumber,
                                                                                                      TradeAction tradeAction,
                                                                                                      int pageSize,
                                                                                                      StockTransactionKey exclusiveStartKey) {

        List<StockTransaction> results = new ArrayList<>();

        QueryResultPage<StockTransaction, StockTransactionKey> queryResultPage;
        StockTransactionKey nextQueryKey = exclusiveStartKey;
        do {
            queryResultPage =
                    doFindStockTransactionByAccountNumberAndTradeAction(accountNumber, tradeAction, pageSize, nextQueryKey);
            results.addAll(queryResultPage.getResults());
            nextQueryKey = queryResultPage.getNextQueryKey();
        } while (results.size() < pageSize && nonNull(nextQueryKey));

        if (results.size() > pageSize) {
            StockTransaction lastItem = results.get(pageSize - 1);

            return QueryResultPage.<StockTransaction, StockTransactionKey>builder()
                    .results(results.subList(0, pageSize))
                    .nextQueryKey(StockTransactionKey.builder()
                    .accountNumber(lastItem.getAccountNumber())
                    .timestamp(lastItem.getTimestamp())
                    .build())
                    .build();
        } else {
            return QueryResultPage.<StockTransaction, StockTransactionKey>builder()
                    .results(results)
                    .nextQueryKey(queryResultPage.getNextQueryKey())
                    .build();
        }

    }

    public QueryResultPage<StockTransaction, StockTransactionKey> doFindStockTransactionByAccountNumberAndTradeAction(String accountNumber,
                                                                                   TradeAction tradeAction,
                                                                                   int pageSize,
                                                                                   StockTransactionKey exclusiveStartKey) {
        Map<String, AttributeValue> attrValues = Map.of(
                ":accountNumber", AttributeValue.builder().s(accountNumber).build(),
                ":tradeAction", AttributeValue.builder().s(tradeAction.name()).build()
        );

        Map<String, AttributeValue> exclusiveStartKeyAttrValues = null;
        if (nonNull(exclusiveStartKey)) {
            exclusiveStartKeyAttrValues = Map.of(
                    "accountNumber", AttributeValue.builder().s(exclusiveStartKey.getAccountNumber()).build(),
                    "timestamp", AttributeValue.builder().s(exclusiveStartKey.getTimestampFormatted()).build()
            );
        }

        QueryRequest queryReq = QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("accountNumber = :accountNumber")
                .filterExpression("tradeAction = :tradeAction")
                .expressionAttributeValues(attrValues)
                .exclusiveStartKey(exclusiveStartKeyAttrValues)
                .limit(pageSize)
                .build();

        QueryResponse response = dynamoDbClient.query(queryReq);

        return QueryResultPage
                .<StockTransaction, StockTransactionKey>builder()
                .results(response.items().stream()
                        .map(StockTransaction::toStockTransaction)
                        .toList())
                .nextQueryKey(mapToStockTransactionKey(response))
                .build();
    }
}
