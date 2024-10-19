package space.gavinklfong.stock.service;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.stereotype.Service;
import space.gavinklfong.stock.dao.DynamoDBDao;
import space.gavinklfong.stock.dto.Show;
import space.gavinklfong.stock.dto.Ticket;
import space.gavinklfong.stock.model.ShowItem;
import space.gavinklfong.stock.model.StockTransaction;
import space.gavinklfong.stock.model.TicketItem;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RequiredArgsConstructor
@Service
public class StockService {

    private final DynamoDBDao dynamoDBDao;

    public List<StockTransaction> findStockTransactions(String accountNumber) {
        return dynamoDBDao.findStockTransactionByAccountNumber(accountNumber);
    }

    public void saveStockTransaction(StockTransaction stockTransaction) {
        dynamoDBDao.saveStockTransaction(stockTransaction);
    }
}
