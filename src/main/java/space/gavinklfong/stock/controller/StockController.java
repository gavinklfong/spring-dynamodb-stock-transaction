package space.gavinklfong.stock.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import space.gavinklfong.stock.model.StockTransaction;
import space.gavinklfong.stock.service.StockService;

import java.util.List;

@RequiredArgsConstructor
@RestController
public class StockController {

    private final StockService stockService;

    @GetMapping("/accounts/{accountNumber}/stocks/transactions")
    public List<StockTransaction> retrieveStockTransactions(@PathVariable String accountNumber) {
        return stockService.findStockTransactions(accountNumber);
    }
}
