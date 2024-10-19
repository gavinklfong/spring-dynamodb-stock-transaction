package space.gavinklfong.stock.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Configuration
public class DynamoDBConfig {

    @Bean
    public DynamoDbClient DynamoDBTableBuilder() {
        return DynamoDbClient.builder()
                .region(Region.US_EAST_2)
                .build();
    }
}
