# Build stage
#
FROM maven:3.6.0-jdk-11-slim AS build
COPY ./pom.xml /home/app/pom.xml
COPY ./avro /home/app/avro
COPY ./bom /home/app/bom
COPY ./common /home/app/common
COPY ./services /home/app/services
WORKDIR /home/app
RUN mvn clean package

#
# Package stage
#
FROM azul/zulu-openjdk-alpine:11
WORKDIR /usr/src/project
VOLUME /config
COPY --from=build /home/app/services/score-aggregator/target/*-jar-with-dependencies.jar /usr/src/project/
CMD java -jar /usr/src/project/score-aggregator-1.0-SNAPSHOT-jar-with-dependencies.jar /config/kafka.properties
