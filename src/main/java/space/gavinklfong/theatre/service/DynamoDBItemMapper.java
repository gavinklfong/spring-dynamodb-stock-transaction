package space.gavinklfong.theatre.service;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import space.gavinklfong.theatre.dto.Show;
import space.gavinklfong.theatre.dto.Ticket;
import space.gavinklfong.theatre.model.ShowItem;
import space.gavinklfong.theatre.model.TicketItem;

@Mapper
public interface DynamoDBItemMapper {

    DynamoDBItemMapper INSTANCE = Mappers.getMapper(DynamoDBItemMapper.class);

    String TICKET_PREFIX = "TICKET#";

    Show mapFromItem(ShowItem item);

    @Mapping(target = "id", expression = "java( item.getSortKey().substring(item.getSortKey().lastIndexOf(TICKET_PREFIX) + 1) )")
    @Mapping(target = "reference", source = "ticketRef")
    Ticket mapFromItem(TicketItem item);
}
