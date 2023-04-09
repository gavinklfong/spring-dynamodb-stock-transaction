package space.gavinklfong.event.service;

import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import space.gavinklfong.event.model.Show;

@Service
public class DynamoDBDao {

    private final DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .region(Region.US_EAST_2)
            .credentialsProvider(ProfileCredentialsProvider.create())
            .build();

    private final DynamoDbEnhancedClient dynamoDbEnhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(dynamoDbClient)
            .build();

    public void insertShow(Show show) {
        DynamoDbTable<Show> showTable = dynamoDbEnhancedClient.table("theatre-show-ticket",
                TableSchema.fromBean(Show.class));

        PutItemEnhancedRequest<Show> request = PutItemEnhancedRequest.builder(Show.class)
                .item(show)
                .build();

        showTable.putItem(request);
    }
}
