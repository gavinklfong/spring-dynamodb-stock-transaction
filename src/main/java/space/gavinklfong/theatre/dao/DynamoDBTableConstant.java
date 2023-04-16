package space.gavinklfong.theatre.dao;

public interface DynamoDBTableConstant {
    String TABLE_NAME = "theatre-ticket";
    String TICKET_REF_INDEX = "ticket-ref-index";
    String SHOW_ITEM_SORT_KEY = "SHOW";
}
