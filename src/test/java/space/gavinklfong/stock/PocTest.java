package space.gavinklfong.theatre;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Slf4j
public class PocTest {

    @Test
    void test() {
        Instant instant = Instant.parse("2024-10-01T09:35:26.2837Z");
        log.info("{}", instant.getNano());
        log.info("{}", instant.getEpochSecond());

        log.info("{}", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS")
                        .withZone(ZoneId.systemDefault())
                .format(instant));
    }
}
