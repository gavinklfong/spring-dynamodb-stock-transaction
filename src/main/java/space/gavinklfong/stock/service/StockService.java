package space.gavinklfong.stock.service;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.stereotype.Service;
import space.gavinklfong.stock.dao.DynamoDBDao;
import space.gavinklfong.stock.dto.Show;
import space.gavinklfong.stock.dto.Ticket;
import space.gavinklfong.stock.model.ShowItem;
import space.gavinklfong.stock.model.TicketItem;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

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

    public List<Ticket> findTickets(String showId) {
        return dynamoDBDao.findShowAndTicketsById(showId).getRight().stream()
                        .map(DynamoDBItemMapper.INSTANCE::mapFromItem)
                .toList();
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
