package space.gavinklfong.event.service;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import space.gavinklfong.event.model.ShowItem;
import space.gavinklfong.event.model.TicketItem;
import space.gavinklfong.event.model.TicketStatus;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;

public class DynamoItemMapper {

    private DynamoItemMapper() {
        super();
    }

    public static ShowItem mapShowItem(Map<String, AttributeValue> item) {
        ShowItem.ShowItemBuilder builder = ShowItem.builder();
        Set<Map.Entry<String, AttributeValue>> entries = item.entrySet();
        entries.forEach(entry -> mapItem(builder, entry));
        return builder.build();
    }

    public static TicketItem mapTicketItem(Map<String, AttributeValue> item) {
        TicketItem.TicketItemBuilder builder = TicketItem.builder();
        Set<Map.Entry<String, AttributeValue>> entries = item.entrySet();
        entries.forEach(entry -> mapItem(builder, entry));
        return builder.build();
    }

    private static void mapItem(TicketItem.TicketItemBuilder builder, Map.Entry<String, AttributeValue> entry) {
        switch (entry.getKey()) {
            case "showId" -> builder.showId(entry.getValue().s());
            case "sortKey" -> builder.sortKey(entry.getValue().s());
            case "ticketRef" -> builder.ticketRef(entry.getValue().s());
            case "status" -> builder.status(TicketStatus.valueOf(entry.getValue().s()));
            case "price" -> builder.price(Double.parseDouble(entry.getValue().n()));
            default -> throw new IllegalArgumentException("unknown ticket item attribute: " + entry.getKey());
        }
    }

    private static void mapItem(ShowItem.ShowItemBuilder builder, Map.Entry<String, AttributeValue> entry) {
        switch (entry.getKey()) {
            case "showId" -> builder.showId(entry.getValue().s());
            case "venue" -> builder.venue(entry.getValue().s());
            case "dateTime" -> builder.dateTime(LocalDateTime.parse(entry.getValue().s()));
            case "name" -> builder.name(entry.getValue().s());
            case "durationInMinute" -> builder.durationInMinute(Integer.parseInt(entry.getValue().n()));
            case "sortKey" -> {}
            default -> throw new IllegalArgumentException("unknown show item attribute: " + entry.getKey());
        }
    }

}
