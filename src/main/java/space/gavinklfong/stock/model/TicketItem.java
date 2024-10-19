package space.gavinklfong.theatre.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.numberValue;
import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.stringValue;

@Builder(toBuilder = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TicketItem {
    String showId;
    String sortKey;
    TicketStatus status;
    SeatArea area;
    Double price;
    String ticketRef;

    public Map<String, AttributeValue> toAttributeValues() {
        Map<String, AttributeValue> attributeValueMap = new HashMap<>(Map.of(
                "showId", stringValue(showId),
                "sortKey", stringValue(sortKey),
                "status", stringValue(status.name()),
                "area", stringValue(area.name()),
                "price", numberValue(price)
        ));
        if (!isEmpty(ticketRef)) {
            attributeValueMap.put("ticketRef", stringValue(ticketRef));
        }
        return attributeValueMap;
    }
}
