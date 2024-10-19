package space.gavinklfong.stock.dto;

import lombok.Builder;
import lombok.Value;

import java.time.LocalDateTime;

@Builder
@Value
public class Show {
    String showId;
    String name;
    LocalDateTime dateTime;
    Integer durationInMinute;
    String venue;
}
