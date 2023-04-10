package space.gavinklfong.event.model;

import lombok.*;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.time.LocalDateTime;

@DynamoDbBean
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ShowItem {
    String showId;
    String sortKey;
    String name;
    LocalDateTime dateTime;
    Integer durationInMinute;
    String venue;

    @DynamoDbPartitionKey
    public void setShowId(String showId) {
        this.showId = showId;
    }

    @DynamoDbSortKey
    public void setSortKey(String sortKey) {
        this.sortKey = sortKey;
    }
}
