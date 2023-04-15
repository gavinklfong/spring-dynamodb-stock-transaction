package space.gavinklfong.theatre.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TicketItem {
    String showId;
    String sortKey;
    TicketStatus status;
    String area;
    Double price;
    String ticketRef;

    @DynamoDbPartitionKey
    public void setShowId(String showId) {
        this.showId = showId;
    }

    @DynamoDbSortKey
    public void setSortKey(String sortKey) {
        this.sortKey = sortKey;
    }
}
