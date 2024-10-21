package space.gavinklfong.stock.dao;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import space.gavinklfong.stock.model.QueryResultPage;
import space.gavinklfong.stock.model.StockTransaction;
import space.gavinklfong.stock.model.StockTransactionKey;
import space.gavinklfong.stock.model.TradeAction;
import space.gavinklfong.stock.util.StockTransactionReader;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class StockTransactionDaoContainerTest {

    private static final String ACCOUNT_NUMBER = "91245776";
    private static final Instant START_TIME = Instant.parse("2022-02-11T00:00:00Z");
    private static final Instant END_TIME = Instant.parse("2022-02-14T00:00:00Z");

    private static final StockTransactionTableBuilder STOCK_TRANSACTION_TABLE_BUILDER = new StockTransactionTableBuilder(DynamoDBTestContainerSetup.DYNAMO_DB_CLIENT);
    private final StockTransactionDao stockTransactionDao = new StockTransactionDao(DynamoDBTestContainerSetup.DYNAMO_DB_CLIENT);

    private static List<StockTransaction> STOCK_TRANSACTIONS;

    @BeforeAll
    static void setupAll() throws IOException {
        STOCK_TRANSACTIONS = StockTransactionReader.readFromCSV();
    }

    @BeforeEach
    void setUp() {
        try {
            STOCK_TRANSACTION_TABLE_BUILDER.deleteTable();
        } catch (ResourceNotFoundException e) {}

        STOCK_TRANSACTION_TABLE_BUILDER.createTable();

        STOCK_TRANSACTIONS.forEach(stockTransactionDao::saveStockTransaction);
    }

    @Test
    void findStockTransactionsByAccountNumber() {
        List<StockTransaction> stockTransactions = stockTransactionDao.findStockTransactionByAccountNumber(ACCOUNT_NUMBER);
        log.info("count: {}", stockTransactions.size());
    }

    @Test
    void findStockTransactionsByAccountNumberAndTimeRange() {
        List<StockTransaction> stockTransactions = stockTransactionDao
                .findStockTransactionByAccountNumberWithTimeRange(ACCOUNT_NUMBER, START_TIME, END_TIME);
        log.info("count: {}", stockTransactions.size());
    }

    @Test
    void findStockTransactionByAccountNumberWithPageSize1() {
        QueryResultPage<StockTransaction, StockTransactionKey> result = stockTransactionDao
                .findStockTransactionByAccountNumber(ACCOUNT_NUMBER, 10,
                        StockTransactionKey.builder()
                                .accountNumber(ACCOUNT_NUMBER)
                                .timestamp(Instant.parse("2022-01-11T14:07:28.282Z"))
                                .build());

        log.info("next query key: {}", result.getNextQueryKey());
        result.getResults().forEach(stockTransaction -> log.info("result: {}", stockTransaction));
    }

    @Test
    void findStockTransactionByAccountNumberWithPageSize() {
        QueryResultPage<StockTransaction, StockTransactionKey> result = stockTransactionDao
                .findStockTransactionByAccountNumber(ACCOUNT_NUMBER, 10, null);

        log.info("next query key: {}", result.getNextQueryKey());
        result.getResults().forEach(stockTransaction -> log.info("result: {}", stockTransaction));

        QueryResultPage<StockTransaction, StockTransactionKey> result2 = stockTransactionDao
                .findStockTransactionByAccountNumber(ACCOUNT_NUMBER, 10, result.getNextQueryKey());

        log.info("next query key: {}", result2.getNextQueryKey());
        result2.getResults().forEach(stockTransaction -> log.info("result: {}", stockTransaction));
    }

    @Test
    void findStockTransactionByAccountNumberAndTradeAction() {
        QueryResultPage<StockTransaction, StockTransactionKey> result = stockTransactionDao
                .findStockTransactionByAccountNumberAndTradeAction(ACCOUNT_NUMBER, TradeAction.SELL);

        assertThat(result.getResults()).hasSize(4);
    }

    @Test
    void findStockTransactionByAccountNumberAndTradeAction_withPagination() {
        QueryResultPage<StockTransaction, StockTransactionKey> result = stockTransactionDao
                .findStockTransactionByAccountNumberAndTradeAction(ACCOUNT_NUMBER, TradeAction.SELL, 10, null);

        assertThat(result.getResults()).hasSize(10);
    }
}
