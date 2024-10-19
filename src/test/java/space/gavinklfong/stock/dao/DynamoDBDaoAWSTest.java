package space.gavinklfong.stock.dao;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DynamoDBDaoAWSTest {

//    private final DynamoDBTableBuilder dynamoDBTableBuilder = new DynamoDBTableBuilder(DynamoDBTestContainerSetup.DYNAMO_DB_CLIENT);
    private final DynamoDBDao dynamoDBDao = new DynamoDBDao(
            DynamoDbClient.builder()
            .region(Region.US_EAST_2)
                .build());

//    @BeforeEach
//    void setUp() {
//        try {
//            dynamoDBTableBuilder.deleteTable();
//        } catch (ResourceNotFoundException e) {}
//
//        dynamoDBTableBuilder.createTable();
//    }

//    @Test
//    void insertShowItem() {
//        String showId = UUID.randomUUID().toString();
//        ShowItem insertedShowItem = ShowItem.builder()
//                .name("The Lion King")
//                .showId(showId)
//                .venue("Lyceum Theatre")
//                .durationInMinute(70)
//                .dateTime(LocalDateTime.parse("2023-05-23T14:00:00"))
//                .build();
//
//        dynamoDBDao.saveShow(insertedShowItem);
//
//        Optional<ShowItem>  retrievedShowItem = dynamoDBDao.findShowById(showId);
//        assertThat(retrievedShowItem)
//                .isPresent()
//                .hasValue(insertedShowItem);
//    }
//
//    @Test
//    void retrieveShowItem_notFound() {
//        Optional<ShowItem> retrievedShowItem = dynamoDBDao.findShowById(UUID.randomUUID().toString());
//        assertThat(retrievedShowItem).isNotPresent();
//    }
//
//    @Test
//    void retrieveTicketItem_notFound() {
//        Optional<TicketItem> retrievedTicketItem = dynamoDBDao.findTicketById(UUID.randomUUID().toString(), UUID.randomUUID().toString());
//        assertThat(retrievedTicketItem).isNotPresent();
//    }
//
//    @Test
//    void retrieveShowAndTicketItems_notFound() {
//        ImmutablePair<ShowItem, List<TicketItem>> showAndTickets = dynamoDBDao.findShowAndTicketsById(UUID.randomUUID().toString());
//
//        assertThat(showAndTickets).extracting(ImmutablePair::getRight).isEqualTo(emptyList());
//        assertThat(showAndTickets).extracting(ImmutablePair::getLeft).isEqualTo(ShowItem.builder().build());
//    }
//
//    @Test
//    void insertTicketItem() {
//        TicketItem insertedTicketItem = TicketItem.builder()
//                .price(RandomUtils.nextDouble(10, 1000))
//                .status(TicketStatus.AVAILABLE)
//                .area(SeatArea.BALCONY)
//                .sortKey(RandomStringUtils.randomAlphanumeric(3))
//                .showId(UUID.randomUUID().toString())
//                .build();
//
//        dynamoDBDao.saveTicket(insertedTicketItem);
//
//        Optional<TicketItem> retrievedTicketItem = dynamoDBDao.findTicketById(insertedTicketItem.getShowId(), insertedTicketItem.getSortKey());
//        assertThat(retrievedTicketItem).isPresent()
//                .hasValue(insertedTicketItem);
//    }
//
//    @Test
//    void retrieveShowAndTicketItems() {
//        String showId = UUID.randomUUID().toString();
//        ShowItem insertedShowItem = ShowItem.builder()
//                .name("The Lion King")
//                .showId(showId)
//                .venue("Lyceum Theatre")
//                .durationInMinute(70)
//                .dateTime(LocalDateTime.parse("2023-05-23T14:00:00"))
//                .build();
//
//        dynamoDBDao.saveShow(insertedShowItem);
//
//        TicketItem insertedTicketItem1 = TicketItem.builder()
//                .price(RandomUtils.nextDouble(10, 1000))
//                .status(TicketStatus.AVAILABLE)
//                .area(SeatArea.BALCONY)
//                .sortKey(RandomStringUtils.randomAlphanumeric(3))
//                .showId(showId)
//                .build();
//
//        dynamoDBDao.saveTicket(insertedTicketItem1);
//
//        TicketItem insertedTicketItem2 = TicketItem.builder()
//                .price(RandomUtils.nextDouble(10, 1000))
//                .status(TicketStatus.RESERVED)
//                .ticketRef(UUID.randomUUID().toString())
//                .area(SeatArea.STALLS)
//                .sortKey(RandomStringUtils.randomAlphanumeric(3))
//                .showId(showId)
//                .build();
//
//        dynamoDBDao.saveTicket(insertedTicketItem2);
//
//        ImmutablePair<ShowItem, List<TicketItem>> showAndTickets = dynamoDBDao.findShowAndTicketsById(insertedTicketItem1.getShowId());
//
//        assertThat(showAndTickets).extracting(ImmutablePair::getRight).isEqualTo(List.of(insertedTicketItem1, insertedTicketItem2));
//        assertThat(showAndTickets).extracting(ImmutablePair::getLeft).isEqualTo(insertedShowItem);
//    }
//
//    @Test
//    void retrieveTicketItemByReference() {
//        String showId = UUID.randomUUID().toString();
//        TicketItem insertedTicketItem1 = TicketItem.builder()
//                .price(RandomUtils.nextDouble(10, 1000))
//                .status(TicketStatus.AVAILABLE)
//                .area(SeatArea.BALCONY)
//                .sortKey(RandomStringUtils.randomAlphanumeric(3))
//                .showId(showId)
//                .build();
//
//        dynamoDBDao.saveTicket(insertedTicketItem1);
//
//        TicketItem insertedTicketItem2 = TicketItem.builder()
//                .price(RandomUtils.nextDouble(10, 1000))
//                .status(TicketStatus.RESERVED)
//                .ticketRef(UUID.randomUUID().toString())
//                .area(SeatArea.STALLS)
//                .sortKey(RandomStringUtils.randomAlphanumeric(3))
//                .showId(showId)
//                .build();
//
//        dynamoDBDao.saveTicket(insertedTicketItem2);
//
//        Optional<TicketItem> retievedTicketItem = dynamoDBDao.findTicketByReference(
//                insertedTicketItem2.getShowId(), insertedTicketItem2.getTicketRef());
//
//        assertThat(retievedTicketItem)
//                .isPresent()
//                .hasValue(insertedTicketItem2);
//    }
//
//    @Test
//    void reserveTicketItem() {
//        String showId = UUID.randomUUID().toString();
//        TicketItem insertedTicketItem1 = TicketItem.builder()
//                .price(RandomUtils.nextDouble(10, 1000))
//                .status(TicketStatus.AVAILABLE)
//                .area(SeatArea.BALCONY)
//                .sortKey(RandomStringUtils.randomAlphanumeric(3))
//                .showId(showId)
//                .build();
//
//        dynamoDBDao.saveTicket(insertedTicketItem1);
//
//        TicketItem insertedTicketItem2 = TicketItem.builder()
//                .price(RandomUtils.nextDouble(10, 1000))
//                .status(TicketStatus.RESERVED)
//                .ticketRef(UUID.randomUUID().toString())
//                .area(SeatArea.STALLS)
//                .sortKey(RandomStringUtils.randomAlphanumeric(3))
//                .showId(showId)
//                .build();
//
//        dynamoDBDao.saveTicket(insertedTicketItem2);
//
//        String ticketRef = UUID.randomUUID().toString();
//        dynamoDBDao.reserveTicket(insertedTicketItem1.getShowId(), insertedTicketItem1.getSortKey(), ticketRef);
//
//        Optional<TicketItem> retievedTicketItem = dynamoDBDao.findTicketByReference(
//                insertedTicketItem1.getShowId(), ticketRef);
//
//        assertThat(retievedTicketItem)
//                .isPresent()
//                .hasValue(insertedTicketItem1.toBuilder()
//                        .ticketRef(ticketRef)
//                        .status(TicketStatus.RESERVED)
//                        .build());
//    }
//
//    @Test
//    void reserveTicketItem_alreadyReserved() {
//        String showId = UUID.randomUUID().toString();
//        TicketItem insertedTicketItem = TicketItem.builder()
//                .price(RandomUtils.nextDouble(10, 1000))
//                .status(TicketStatus.RESERVED)
//                .ticketRef(UUID.randomUUID().toString())
//                .area(SeatArea.STALLS)
//                .sortKey(RandomStringUtils.randomAlphanumeric(3))
//                .showId(showId)
//                .build();
//
//        dynamoDBDao.saveTicket(insertedTicketItem);
//
//        String ticketRef = UUID.randomUUID().toString();
//        assertThrows(TicketReservationException.class,
//                () -> dynamoDBDao.reserveTicket(insertedTicketItem.getShowId(), insertedTicketItem.getSortKey(), ticketRef));
//    }
//
//
//    @Test
//    void reserveTicketItem_notFound() {
//        assertThrows(TicketReservationException.class,
//                () -> dynamoDBDao.reserveTicket(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
//                        UUID.randomUUID().toString()));
//    }

}
