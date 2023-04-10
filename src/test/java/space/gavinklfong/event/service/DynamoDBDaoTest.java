package space.gavinklfong.event.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import space.gavinklfong.event.model.ShowItem;
import space.gavinklfong.event.model.TicketItem;
import space.gavinklfong.event.model.TicketStatus;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

@Slf4j
class DynamoDBDaoTest {

    private DynamoDBDao dynamoDBDao = new DynamoDBDao();

    @Test
    void createTable() {
        dynamoDBDao.createTable();
    }

    @Test
    void retrieveDataTest() {
        ImmutablePair<ShowItem, List<TicketItem>> pair = dynamoDBDao.retrieveShowTickets("ed021707-743f-49d1-bb04-de6e7abdcdd2");
        log.info("show: {}", pair.getLeft());
        log.info("tickets: {}", pair.getRight());
    }

    @Test
    void createSampleData() {
        String showId = UUID.randomUUID().toString();
        saveShow(showId, "ABBA Voyage", "ABBA Arena");
        IntStream.rangeClosed('A', 'C').forEach(rowId -> saveTicket(showId, (char)rowId + "1", TicketStatus.RESERVED));
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
