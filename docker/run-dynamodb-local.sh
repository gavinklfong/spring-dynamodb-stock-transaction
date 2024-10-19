#/bin/sh

docker run -p 8000:8000 amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -sharedDb