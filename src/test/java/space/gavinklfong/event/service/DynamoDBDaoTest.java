package space.gavinklfong.event.service;

import org.junit.jupiter.api.Test;
import space.gavinklfong.event.model.Show;

import java.time.LocalDateTime;
import java.util.UUID;

class DynamoDBDaoTest {

    @Test
    void insertShowTest() {
        DynamoDBDao dynamoDBDao = new DynamoDBDao();
        Show show = Show.builder()
                .showId(UUID.randomUUID().toString())
                .sortKey("SHOW")
                .venue("ABBA Arena")
                .name("ABBA Voyage")
                .durationInMinute(70)
                .dateTime(LocalDateTime.parse("2023-05-20T12:00"))
                .build();

        dynamoDBDao.insertShow(show);

    }
}
