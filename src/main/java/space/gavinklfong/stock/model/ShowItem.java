package space.gavinklfong.stock.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.numberValue;
import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.stringValue;
import static space.gavinklfong.stock.dao.DynamoDBTableConstant.SHOW_ITEM_SORT_KEY;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ShowItem {
    String showId;
    @Builder.Default
    String sortKey = SHOW_ITEM_SORT_KEY;
    String name;
    LocalDateTime dateTime;
    Integer durationInMinute;
    String venue;

    public Map<String, AttributeValue> toAttributeValues() {
        return Map.of(
                "showId", stringValue(showId),
                "sortKey", stringValue(sortKey),
                "name", stringValue(name),
                "dateTime", stringValue(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(dateTime)),
                "durationInMinute", numberValue(durationInMinute),
                "venue", stringValue(venue)
        );
    }
}
