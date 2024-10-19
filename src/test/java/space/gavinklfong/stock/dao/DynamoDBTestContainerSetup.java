package space.gavinklfong.theatre.dao;

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;

public class DynamoDBTestContainerSetup {

    static final GenericContainer DYNAMODB_CONTAINER;

    static {
        DYNAMODB_CONTAINER = new GenericContainer(DockerImageName.parse("amazon/dynamodb-local:latest"))
                .withCommand("-jar", "DynamoDBLocal.jar", "-sharedDb")
                .withExposedPorts(8000)
                .withLogConsumer(new Slf4jLogConsumer((LoggerFactory.getLogger(DynamoDBTestContainerSetup.class))));
        DYNAMODB_CONTAINER.start();
    }

    public static DynamoDbClient DYNAMO_DB_CLIENT = DynamoDbClient.builder()
                .region(Region.US_EAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummy", "dummy")))
                .endpointOverride(URI.create(String.format("http://localhost:%d", DYNAMODB_CONTAINER.getMappedPort(8000))))
                .build();

}
