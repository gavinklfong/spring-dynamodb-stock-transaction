package space.gavinklfong.theatre.dao;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import space.gavinklfong.theatre.model.ShowItem;
import space.gavinklfong.theatre.model.TicketItem;
import space.gavinklfong.theatre.model.TicketStatus;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.stream.IntStream;

@Slf4j
class DynamoDBTableBuilderTest {

    private DynamoDBTableBuilder dynamoDBTableBuilder = new DynamoDBTableBuilder();

    private DynamoDBDao dynamoDBDao = new DynamoDBDao();

    @Test
    void createTable() {
        dynamoDBTableBuilder.createTable();
    }

    @Test
    void deleteTable() {
        dynamoDBTableBuilder.deleteTable();
    }


    @Test
    void createSampleData() {
        String showId = UUID.randomUUID().toString();
        saveShow(showId, "ABBA Voyage", "ABBA Arena");
        IntStream.rangeClosed('A', 'C').forEach(rowId -> saveTicket(showId, (char)rowId + "1", TicketStatus.RESERVED));
        IntStream.rangeClosed('E', 'G').forEach(rowId -> saveTicket(showId, (char)rowId + "1", TicketStatus.AVAILABLE));
    }

    private void saveShow(String showId, String name, String venue) {
        ShowItem showItem = ShowItem.builder()
                .showId(showId)
                .sortKey("SHOW")
                .name(name)
                .venue(venue)
                .durationInMinute(70)
                .dateTime(LocalDateTime.parse("2023-05-20T12:00"))
                .build();

        dynamoDBDao.saveShow(showItem);
    }

    private void saveTicket(String showId, String ticketId, TicketStatus ticketStatus) {
        TicketItem.TicketItemBuilder builder = TicketItem.builder()
                .showId(showId)
                .sortKey(ticketId)
                .status(ticketStatus);

        if (TicketStatus.RESERVED.equals(ticketStatus)) {
            builder.ticketRef(UUID.randomUUID().toString());
        }

        dynamoDBDao.saveTicket(builder.build());
    }



}
