package space.gavinklfong.theatre.dao;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import space.gavinklfong.theatre.model.ShowItem;
import space.gavinklfong.theatre.model.TicketItem;

import java.util.List;

@Slf4j
class DynamoDBDaoTest {

    private static final String SHOW_ID = "42aa09ef-7533-4434-8360-0115eafe43b8";

    private DynamoDBDao dynamoDBDao = new DynamoDBDao();

    @Test
    void retrieveDataTest() {
        ImmutablePair<ShowItem, List<TicketItem>> pair = dynamoDBDao.retrieveShowTickets(SHOW_ID);
        log.info("show: {}", pair.getLeft());
        log.info("tickets: {}", pair.getRight());
    }

    @Test
    void reserveTicketTest() {
        dynamoDBDao.reserveTicket(SHOW_ID, "C1");
    }

}
