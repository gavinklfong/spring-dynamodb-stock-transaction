package space.gavinklfong.stock.dao;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.IdempotentParameterMismatchException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import space.gavinklfong.stock.exception.TicketReservationException;
import space.gavinklfong.stock.model.SeatArea;
import space.gavinklfong.stock.model.ShowItem;
import space.gavinklfong.stock.model.TicketItem;
import space.gavinklfong.stock.model.TicketStatus;

import java.time.LocalDateTime;
import java.util.*;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
class DynamoDBDaoContainerTest {

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
    void retrieveShowItem_notFound() {
        Optional<ShowItem> retrievedShowItem = dynamoDBDao.findShowById(UUID.randomUUID().toString());
        assertThat(retrievedShowItem).isNotPresent();
    }

    @Test
    void retrieveTicketItem_notFound() {
        Optional<TicketItem> retrievedTicketItem = dynamoDBDao.findTicketById(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        assertThat(retrievedTicketItem).isNotPresent();
    }

    @Test
    void retrieveShowAndTicketItems_notFound() {
        ImmutablePair<ShowItem, List<TicketItem>> showAndTickets = dynamoDBDao.findShowAndTicketsById(UUID.randomUUID().toString());

        assertThat(showAndTickets).extracting(ImmutablePair::getRight).isEqualTo(emptyList());
        assertThat(showAndTickets).extracting(ImmutablePair::getLeft).isEqualTo(ShowItem.builder().build());
    }

    @Test
    void insertTicketItem() {

        TicketItem insertedTicketItem = insertTicketItem(UUID.randomUUID().toString(), SeatArea.BALCONY,
                RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);

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

        TicketItem insertedTicketItem1 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);
        TicketItem insertedTicketItem2 = insertTicketItem(showId, SeatArea.STALLS, RandomUtils.nextDouble(10, 1000), TicketStatus.RESERVED);

        ImmutablePair<ShowItem, List<TicketItem>> showAndTickets = dynamoDBDao.findShowAndTicketsById(insertedTicketItem1.getShowId());

        assertThat(showAndTickets.getRight()).containsExactlyInAnyOrderElementsOf(List.of(insertedTicketItem1, insertedTicketItem2));
        assertThat(showAndTickets).extracting(ImmutablePair::getLeft).isEqualTo(insertedShowItem);
    }

    @Test
    void retrieveAverageTicketPriceBySeatArea() {
        String showId = UUID.randomUUID().toString();
        ShowItem insertedShowItem = ShowItem.builder()
                .name("The Lion King")
                .showId(showId)
                .venue("Lyceum Theatre")
                .durationInMinute(70)
                .dateTime(LocalDateTime.parse("2023-05-23T14:00:00"))
                .build();

        dynamoDBDao.saveShow(insertedShowItem);

        insertTicketItem(showId, SeatArea.BALCONY, 10D, TicketStatus.AVAILABLE);
        insertTicketItem(showId, SeatArea.BALCONY, 20D, TicketStatus.AVAILABLE);
        insertTicketItem(showId, SeatArea.BALCONY, 30D, TicketStatus.AVAILABLE);

        insertTicketItem(showId, SeatArea.STALLS, 100D, TicketStatus.AVAILABLE);
        insertTicketItem(showId, SeatArea.STALLS, 110D, TicketStatus.AVAILABLE);
        insertTicketItem(showId, SeatArea.STALLS, 120D, TicketStatus.AVAILABLE);
        insertTicketItem(showId, SeatArea.STALLS, 150D, TicketStatus.AVAILABLE);

        Map<SeatArea, Double> averagePriceBySeatArea = dynamoDBDao.findAverageTicketPriceBySeatArea(showId);
        log.info("{}", averagePriceBySeatArea);
    }

    @Test
    void retrieveTicketItemByReference() {
        String showId = UUID.randomUUID().toString();

        TicketItem insertedTicketItem1 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);
        TicketItem insertedTicketItem2 = insertTicketItem(showId, SeatArea.STALLS, RandomUtils.nextDouble(10, 1000), TicketStatus.RESERVED, UUID.randomUUID().toString());

        Optional<TicketItem> retievedTicketItem = dynamoDBDao.findTicketByReference(
                insertedTicketItem2.getShowId(), insertedTicketItem2.getTicketRef());

        assertThat(retievedTicketItem)
                .isPresent()
                .hasValue(insertedTicketItem2);
    }

    @Test
    void reserveTicketItem() {
        String showId = UUID.randomUUID().toString();
        TicketItem availableTicket = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);
        TicketItem reservedTicket = insertTicketItem(showId, SeatArea.STALLS, RandomUtils.nextDouble(10, 1000), TicketStatus.RESERVED, UUID.randomUUID().toString());

        String ticketRef = UUID.randomUUID().toString();
        dynamoDBDao.reserveTicket(availableTicket.getShowId(), availableTicket.getSortKey(), ticketRef);

        Optional<TicketItem> retrievedTicketItem = dynamoDBDao.findTicketByReference(
                availableTicket.getShowId(), ticketRef);

        assertThat(retrievedTicketItem)
                .isPresent()
                .hasValue(availableTicket.toBuilder()
                        .ticketRef(ticketRef)
                        .status(TicketStatus.RESERVED)
                        .build());
    }

    @Test
    void givenSameTokenForSameRequest_whenReserveMultipleTickets_thenTicketsReservedSuccessfully() {
        String showId = UUID.randomUUID().toString();
        String ticketRef = UUID.randomUUID().toString();

        TicketItem availableTicket1 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);
        TicketItem availableTicket2 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);

        Set<String> ticketIds = Set.of(availableTicket1.getSortKey(), availableTicket2.getSortKey());

        String requestToken = RandomStringUtils.randomAlphanumeric(30).toUpperCase();

        dynamoDBDao.reserveTickets(showId, ticketIds, ticketRef, requestToken);

        assertTicketReservedInDynamoDB(availableTicket1, ticketRef);
        assertTicketReservedInDynamoDB(availableTicket2, ticketRef);

        dynamoDBDao.reserveTickets(showId, ticketIds, ticketRef, requestToken);

        assertTicketReservedInDynamoDB(availableTicket1, ticketRef);
        assertTicketReservedInDynamoDB(availableTicket2, ticketRef);
    }

    @Test
    void givenSameTokenForDifferentRequest_whenReserveMultipleTickets_thenTicketsReservedSuccessfully() {
        String showId = UUID.randomUUID().toString();
        String ticketRef = UUID.randomUUID().toString();

        TicketItem availableTicket1 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);
        TicketItem availableTicket2 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);
        TicketItem availableTicket3 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);

        dynamoDBDao.reserveTickets(showId, Set.of(availableTicket1.getSortKey(), availableTicket2.getSortKey()), ticketRef, "request-token-1");

        assertTicketReservedInDynamoDB(availableTicket1, ticketRef);
        assertTicketReservedInDynamoDB(availableTicket2, ticketRef);

        assertThatThrownBy(() -> dynamoDBDao.reserveTickets(showId, Set.of(availableTicket3.getSortKey()), ticketRef, "request-token-1"))
                .isInstanceOf(IdempotentParameterMismatchException.class);

        assertEqualTicketInDynamoDB(availableTicket3);
    }

    @Test
    void givenDifferentTokenForSameRequest_whenReserveMultipleTickets_thenTicketsReservedSuccessfully() {
        String showId = UUID.randomUUID().toString();
        String ticketRef = UUID.randomUUID().toString();

        TicketItem availableTicket1 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);
        TicketItem availableTicket2 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);

        Set<String> ticketIds = Set.of(availableTicket1.getSortKey(), availableTicket2.getSortKey());

        String requestToken1 = RandomStringUtils.randomAlphanumeric(30).toUpperCase();
        dynamoDBDao.reserveTickets(showId, ticketIds, ticketRef, requestToken1);

        assertTicketReservedInDynamoDB(availableTicket1, ticketRef);
        assertTicketReservedInDynamoDB(availableTicket2, ticketRef);

        String requestToken2 = RandomStringUtils.randomAlphanumeric(30).toUpperCase();
        assertThatThrownBy(() -> dynamoDBDao.reserveTickets(showId, ticketIds, ticketRef, requestToken2))
                .isInstanceOf(TransactionCanceledException.class);
    }

    @Test
    void givenAllTicketAvailable_whenReserveMultipleTickets_thenTicketsReservedSuccessfully() {
        String showId = UUID.randomUUID().toString();
        String ticketRef = UUID.randomUUID().toString();

        TicketItem availableTicket1 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);
        TicketItem availableTicket2 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);

        Set<String> ticketIds = Set.of(availableTicket1.getSortKey(), availableTicket2.getSortKey());

        dynamoDBDao.reserveTickets(showId, ticketIds, ticketRef);

        assertTicketReservedInDynamoDB(availableTicket1, ticketRef);
        assertTicketReservedInDynamoDB(availableTicket2, ticketRef);
    }

    @Test
    void givenOneTicketNotAvailable_whenReserveMultipleTickets_thenTransactionCanceled() {
        String showId = UUID.randomUUID().toString();
        String ticketRef = UUID.randomUUID().toString();

        TicketItem availableTicket1 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);
        TicketItem availableTicket2 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.AVAILABLE);
        TicketItem reservedTicket1 = insertTicketItem(showId, SeatArea.BALCONY, RandomUtils.nextDouble(10, 1000), TicketStatus.RESERVED);

        Set<String> ticketIds = Set.of(availableTicket1.getSortKey(), availableTicket2.getSortKey(), reservedTicket1.getSortKey());

        assertThatThrownBy(() -> dynamoDBDao.reserveTickets(showId, ticketIds, ticketRef))
                .isInstanceOf(TransactionCanceledException.class);

        assertEqualTicketInDynamoDB(availableTicket1);
        assertEqualTicketInDynamoDB(availableTicket2);
        assertEqualTicketInDynamoDB(reservedTicket1);
    }

    private void assertTicketReservedInDynamoDB(TicketItem ticketItem, String ticketRef) {
        Optional<TicketItem> retrievedTicketItem = dynamoDBDao.findTicketById(ticketItem.getShowId(), ticketItem.getSortKey());

        assertThat(retrievedTicketItem)
                .isPresent()
                .hasValue(ticketItem.toBuilder()
                        .ticketRef(ticketRef)
                        .status(TicketStatus.RESERVED)
                        .build());
    }


    private void assertEqualTicketInDynamoDB(TicketItem ticketItem) {
        Optional<TicketItem> retrievedTicketItem = dynamoDBDao.findTicketById(ticketItem.getShowId(), ticketItem.getSortKey());

        assertThat(retrievedTicketItem)
                .isPresent()
                .hasValue(ticketItem);
    }

    @Test
    void reserveTicketItem_alreadyReserved() {
        String showId = UUID.randomUUID().toString();
        TicketItem insertedTicketItem = TicketItem.builder()
                .price(RandomUtils.nextDouble(10, 1000))
                .status(TicketStatus.RESERVED)
                .ticketRef(UUID.randomUUID().toString())
                .area(SeatArea.STALLS)
                .sortKey(RandomStringUtils.randomAlphanumeric(3))
                .showId(showId)
                .build();

        dynamoDBDao.saveTicket(insertedTicketItem);

        String ticketRef = UUID.randomUUID().toString();
        assertThrows(TicketReservationException.class,
                () -> dynamoDBDao.reserveTicket(insertedTicketItem.getShowId(), insertedTicketItem.getSortKey(), ticketRef));
    }

    @Test
    void reserveTicketItem_notFound() {
        assertThrows(TicketReservationException.class,
                () -> dynamoDBDao.reserveTicket(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                        UUID.randomUUID().toString()));
    }

    private TicketItem insertTicketItem(String showId, SeatArea seatArea, double price, TicketStatus ticketStatus) {
        return insertTicketItem(showId, seatArea, price, ticketStatus, null);
    }

    private TicketItem insertTicketItem(String showId, SeatArea seatArea, double price, TicketStatus ticketStatus, String ticketRef) {
        TicketItem insertedTicketItem = TicketItem.builder()
                .price(price)
                .status(ticketStatus)
                .area(seatArea)
                .sortKey(String.format("TICKET#%s", RandomStringUtils.randomAlphanumeric(3).toUpperCase()))
                .showId(showId)
                .ticketRef(ticketRef)
                .build();

        dynamoDBDao.saveTicket(insertedTicketItem);

        return insertedTicketItem;
    }

}
