package space.gavinklfong.event.dto;

import lombok.Builder;
import lombok.Value;

import java.time.LocalDateTime;

@Builder
@Value
public class Show {
    String name;
    String description;
    String location;
    LocalDateTime dateTime;
}
