package space.gavinklfong.stock.dto;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class Ticket {
    String id;
    String status;
    Double price;
    String area;
    String reference;
}
