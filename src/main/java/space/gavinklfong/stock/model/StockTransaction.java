package space.gavinklfong.stock.model;

import lombok.Builder;
import lombok.Value;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.numberValue;
import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.stringValue;

@Builder(toBuilder = true)
@Value
public class StockTransaction {
    String accountNumber;
    Instant timestamp;
    String ticker;
    TradeAction tradeAction;
    int unit;
    BigDecimal unitPrice;
    String reference;

    public Map<String, AttributeValue> toAttributeValues() {
        Map<String, AttributeValue> attributeValueMap = new HashMap<>(Map.of(
                "accountNumber", stringValue(accountNumber),
                "timestamp", stringValue(DateTimeFormatter.ISO_INSTANT.format(timestamp)),
                "ticker", stringValue(ticker),
                "tradeAction", stringValue(tradeAction.name()),
                "unit", numberValue(unit),
                "unitPrice", numberValue(unitPrice),
                "reference", stringValue(reference)
        ));
        return attributeValueMap;
    }

    public static StockTransaction toStockTransaction(Map<String, AttributeValue> item) {
        StockTransaction.StockTransactionBuilder builder = StockTransaction.builder();
        Set<Map.Entry<String, AttributeValue>> entries = item.entrySet();
        entries.forEach(entry -> mapItem(builder, entry));
        return builder.build();
    }

    private static void mapItem(StockTransaction.StockTransactionBuilder builder, Map.Entry<String, AttributeValue> entry) {
        switch (entry.getKey()) {
            case "accountNumber" -> builder.accountNumber(entry.getValue().s());
            case "timestamp" -> builder.timestamp(Instant.parse(entry.getValue().s()));
            case "tradeAction" -> builder.tradeAction(TradeAction.valueOf(entry.getValue().s()));
            case "ticker" -> builder.ticker(entry.getValue().s());
            case "unit" -> builder.unit(Integer.parseInt(entry.getValue().n()));
            case "unitPrice" -> builder.unitPrice(new BigDecimal(entry.getValue().s()));
            case "reference" -> builder.reference(entry.getValue().s());
            default -> throw new IllegalArgumentException("unknown stock transaction attribute: " + entry.getKey());
        }
    }
}
