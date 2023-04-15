package space.gavinklfong.theatre.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import space.gavinklfong.theatre.dao.DynamoDBDao;
import space.gavinklfong.theatre.dto.Show;
import space.gavinklfong.theatre.dto.Ticket;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

@RequiredArgsConstructor
@Service
public class ShowTicketService {

    private final DynamoDBDao dynamoDBDao;

    public Show retrieveTheatreShow(String showId) {
        return DynamoDBItemMapper.INSTANCE
                .mapFromItem(dynamoDBDao.retrieveShowItem(showId));
    }

    public Map<String, List<Ticket>> retrieveTicketsGroupByArea(String showId) {
        return dynamoDBDao.retrieveShowTickets(showId).getRight().stream()
                        .map(DynamoDBItemMapper.INSTANCE::mapFromItem)
                .collect(groupingBy(Ticket::getArea));
    }

    public void reservedTicket(String showId, String ticketId) {

    }

}
