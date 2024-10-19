package space.gavinklfong.stock.dao;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import space.gavinklfong.stock.model.StockTransaction;
import space.gavinklfong.stock.util.StockTransactionReader;

import java.io.IOException;
import java.util.List;

@Slf4j
class DynamoDBDaoContainerTest {

    private static final String ACCOUNT_NUMBER = "91245776";

    private static final DynamoDBTableBuilder dynamoDBTableBuilder = new DynamoDBTableBuilder(DynamoDBTestContainerSetup.DYNAMO_DB_CLIENT);
    private final DynamoDBDao dynamoDBDao = new DynamoDBDao(DynamoDBTestContainerSetup.DYNAMO_DB_CLIENT);

    private static List<StockTransaction> STOCK_TRANSACTIONS;

    @BeforeAll
    static void setupAll() throws IOException {
        STOCK_TRANSACTIONS = StockTransactionReader.readFromCSV();
    }

    @BeforeEach
    void setUp() {
        try {
            dynamoDBTableBuilder.deleteTable();
        } catch (ResourceNotFoundException e) {}

        dynamoDBTableBuilder.createTable();

        STOCK_TRANSACTIONS.forEach(dynamoDBDao::saveStockTransaction);
    }

    @Test
    void findStockTransactionsByAccountNumber() {
        List<StockTransaction> stockTransactions = dynamoDBDao.findStockTransactionByAccountNumber(ACCOUNT_NUMBER);
        log.info("count: {}", stockTransactions.size());
    }
}
