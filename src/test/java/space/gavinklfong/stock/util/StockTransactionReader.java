package space.gavinklfong.stock.util;

import org.apache.commons.csv.CSVFormat;
import org.springframework.core.io.ClassPathResource;
import space.gavinklfong.stock.model.StockTransaction;
import space.gavinklfong.stock.model.TradeAction;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public class StockTransactionReader {

    private static final String STOCK_TXN_CSV = "stock_transaction.csv";

    private static final CSVFormat CSV_FORMAT = CSVFormat.RFC4180.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .build();

    public static List<StockTransaction> readFromCSV() throws IOException {
        try (Reader in = new FileReader(new ClassPathResource(STOCK_TXN_CSV).getFile())) {
            return CSV_FORMAT.parse(in).stream()
                    .map(csvRecord -> StockTransaction.builder()
                            .accountNumber(csvRecord.get(0))
                            .timestamp(Instant.parse(csvRecord.get(1)))
                            .ticker(csvRecord.get(2))
                            .tradeAction(TradeAction.valueOf(csvRecord.get(3)))
                            .unitPrice(new BigDecimal(csvRecord.get(4)))
                            .unit(Integer.parseInt(csvRecord.get(5)))
                            .build())
                    .toList();
        }
    }
}
