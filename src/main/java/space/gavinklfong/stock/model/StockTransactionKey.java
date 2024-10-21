package space.gavinklfong.stock.model;

import lombok.Builder;
import lombok.Value;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;

@Builder
@Value
public class StockTransactionKey {
    String accountNumber;
    Instant timestamp;

    public String getTimestampFormatted() {
        return DateTimeFormatter.ISO_INSTANT.format(timestamp);
    }

    public static StockTransactionKey toStockTransactionKey(Map<String, AttributeValue> item) {
        StockTransactionKey.StockTransactionKeyBuilder builder = StockTransactionKey.builder();
        Set<Map.Entry<String, AttributeValue>> entries = item.entrySet();
        entries.forEach(entry -> mapItem(builder, entry));
        return builder.build();
    }

    private static void mapItem(StockTransactionKey.StockTransactionKeyBuilder builder, Map.Entry<String, AttributeValue> entry) {
        switch (entry.getKey()) {
            case "accountNumber" -> builder.accountNumber(entry.getValue().s());
            case "timestamp" -> builder.timestamp(Instant.parse(entry.getValue().s()));
            default -> throw new IllegalArgumentException("unknown stock transaction attribute: " + entry.getKey());
        }
    }
}
