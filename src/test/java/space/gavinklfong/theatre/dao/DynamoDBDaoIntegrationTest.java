package space.gavinklfong.theatre.dao;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import space.gavinklfong.theatre.model.SeatArea;
import space.gavinklfong.theatre.model.ShowItem;
import space.gavinklfong.theatre.model.TicketItem;
import space.gavinklfong.theatre.model.TicketStatus;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DynamoDBDaoIntegrationTest {

    private final DynamoDBTableBuilder dynamoDBTableBuilder = new DynamoDBTableBuilder(DynamoDBTestContainerSetup.DYNAMO_DB_CLIENT);
    private final DynamoDBDao dynamoDBDao = new DynamoDBDao(DynamoDBTestContainerSetup.DYNAMO_DB_CLIENT);

    @BeforeEach
    void setUp() {
        try {
            dynamoDBTableBuilder.deleteTable();
        } catch (ResourceNotFoundException e) {}

        dynamoDBTableBuilder.createTable();
    }

    @Test
    void insertShowItem() {
        String showId = UUID.randomUUID().toString();
        ShowItem insertedShowItem = ShowItem.builder()
                .name("The Lion King")
                .showId(showId)
                .venue("Lyceum Theatre")
                .durationInMinute(70)
                .dateTime(LocalDateTime.parse("2023-05-23T14:00:00"))
                .build();

        dynamoDBDao.saveShow(insertedShowItem);

        Optional<ShowItem>  retrievedShowItem = dynamoDBDao.findShowById(showId);
        assertThat(retrievedShowItem)
                .isPresent()
                .hasValue(insertedShowItem);
    }

    @Test
    void insertTicketItem() {
        TicketItem insertedTicketItem = TicketItem.builder()
                .price(RandomUtils.nextDouble(10, 1000))
                .status(TicketStatus.AVAILABLE)
                .area(SeatArea.BALCONY.name())
                .sortKey(RandomStringUtils.randomAlphanumeric(3))
                .showId(UUID.randomUUID().toString())
                .build();

        dynamoDBDao.saveTicket(insertedTicketItem);

        Optional<TicketItem> retrievedTicketItem = dynamoDBDao.findTicketById(insertedTicketItem.getShowId(), insertedTicketItem.getSortKey());
        assertThat(retrievedTicketItem).isPresent()
                .hasValue(insertedTicketItem);
    }

    @Test
    void retrieveShowAndTicketItems() {
        String showId = UUID.randomUUID().toString();
        ShowItem insertedShowItem = ShowItem.builder()
                .name("The Lion King")
                .showId(showId)
                .venue("Lyceum Theatre")
                .durationInMinute(70)
                .dateTime(LocalDateTime.parse("2023-05-23T14:00:00"))
                .build();

        dynamoDBDao.saveShow(insertedShowItem);

        TicketItem insertedTicketItem1 = TicketItem.builder()
                .price(RandomUtils.nextDouble(10, 1000))
                .status(TicketStatus.AVAILABLE)
                .area(SeatArea.BALCONY.name())
                .sortKey(RandomStringUtils.randomAlphanumeric(3))
                .showId(showId)
                .build();

        dynamoDBDao.saveTicket(insertedTicketItem1);

        TicketItem insertedTicketItem2 = TicketItem.builder()
                .price(RandomUtils.nextDouble(10, 1000))
                .status(TicketStatus.RESERVED)
                .ticketRef(UUID.randomUUID().toString())
                .area(SeatArea.STALLS.name())
                .sortKey(RandomStringUtils.randomAlphanumeric(3))
                .showId(showId)
                .build();

        dynamoDBDao.saveTicket(insertedTicketItem2);

        ImmutablePair<ShowItem, List<TicketItem>> showAndTickets = dynamoDBDao.findShowAndTicketsById(insertedTicketItem1.getShowId());

        assertThat(showAndTickets).extracting(ImmutablePair::getRight).isEqualTo(List.of(insertedTicketItem1, insertedTicketItem2));
        assertThat(showAndTickets).extracting(ImmutablePair::getLeft).isEqualTo(insertedShowItem);
    }

    @Test
    void retrieveTicketItemByReference() {
        String showId = UUID.randomUUID().toString();
        TicketItem insertedTicketItem1 = TicketItem.builder()
                .price(RandomUtils.nextDouble(10, 1000))
                .status(TicketStatus.AVAILABLE)
                .area(SeatArea.BALCONY.name())
                .sortKey(RandomStringUtils.randomAlphanumeric(3))
                .showId(showId)
                .build();

        dynamoDBDao.saveTicket(insertedTicketItem1);

        TicketItem insertedTicketItem2 = TicketItem.builder()
                .price(RandomUtils.nextDouble(10, 1000))
                .status(TicketStatus.RESERVED)
                .ticketRef(UUID.randomUUID().toString())
                .area(SeatArea.STALLS.name())
                .sortKey(RandomStringUtils.randomAlphanumeric(3))
                .showId(showId)
                .build();

        dynamoDBDao.saveTicket(insertedTicketItem2);

        Optional<TicketItem> retievedTicketItem = dynamoDBDao.findTicketByReference(
                insertedTicketItem2.getShowId(), insertedTicketItem2.getTicketRef());

        assertThat(retievedTicketItem)
                .isPresent()
                .hasValue(insertedTicketItem2);
    }

}
