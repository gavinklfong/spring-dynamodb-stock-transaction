package space.gavinklfong.theatre.service;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.stereotype.Service;
import space.gavinklfong.theatre.dao.DynamoDBDao;
import space.gavinklfong.theatre.dto.Show;
import space.gavinklfong.theatre.dto.Ticket;
import space.gavinklfong.theatre.model.ShowItem;
import space.gavinklfong.theatre.model.TicketItem;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static java.util.stream.Collectors.groupingBy;

@RequiredArgsConstructor
@Service
public class ShowTicketService {

    private final DynamoDBDao dynamoDBDao;

    public Optional<Show> findTheatreShow(String showId) {
        return dynamoDBDao.findShowById(showId)
                .map(DynamoDBItemMapper.INSTANCE::mapFromItem);
    }

    public ImmutablePair<Show, List<Ticket>> findTheatreShowAndTickets(String showId) {
        ImmutablePair<ShowItem, List<TicketItem>> result = dynamoDBDao.findShowAndTicketsById(showId);
         List<Ticket> tickets = result.getRight().stream()
                .map(DynamoDBItemMapper.INSTANCE::mapFromItem)
                 .toList();

         Show show = DynamoDBItemMapper.INSTANCE.mapFromItem(result.getLeft());

        return ImmutablePair.of(show, tickets);
    }

    public Map<String, List<Ticket>> findTicketsGroupingByArea(String showId) {
        return dynamoDBDao.findShowAndTicketsById(showId).getRight().stream()
                        .map(DynamoDBItemMapper.INSTANCE::mapFromItem)
                .collect(groupingBy(Ticket::getArea));
    }

    public String reserveTicket(String showId, String ticketId) {
        String ticketRef = UUID.randomUUID().toString();
        dynamoDBDao.reserveTicket(showId, ticketId, ticketRef);
        return ticketRef;
    }

    public Optional<Ticket> findTicketByReference(String showId, String reference) {
        return dynamoDBDao.findTicketByReference(showId, reference)
                .map(DynamoDBItemMapper.INSTANCE::mapFromItem);
    }
}
