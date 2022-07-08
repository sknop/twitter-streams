package io.confluent.bootcamp;

import io.confluent.bootcamp.streams.Context;
import io.confluent.bootcamp.streams.SerdeGenerator;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class TwitterPart2Test {

    private TopologyTestDriver driver;
//    private TestInputTopic<String, Status> topicTweetStatus;
    private TestOutputTopic<Void, Void> topicDlq;
    private MockProducer<String, byte[]> dlqProducer;
    private MockAdminClient adminClient;

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException {
        // Building mock clients and schema registry
        dlqProducer = new MockProducer<String, byte[]>();
        adminClient = MockAdminClient.create().numBrokers(1).defaultPartitions((short) 1).defaultReplicationFactor(1).build();
        adminClient.createTopics(Collections.singletonList(new NewTopic(Constant.TOPIC_TWEET, 1, (short) 1)));
        Context.setProducer(dlqProducer);
        Context.setAdminClient(adminClient);
        Context.getConfiguration().put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://local");

        // Inserting topology
        TwitterPart2 twitterPart2 = new TwitterPart2();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        twitterPart2.buildTopology(streamsBuilder);

        // Building topology test driver and topics
        driver = new TopologyTestDriver(streamsBuilder.build());
//        topicTweetStatus = driver.createInputTopic(Constant.TOPIC_TWEET, new StringSerializer(), SerdeGenerator.<Status>getSerde().serializer());
//        topicTweeps = driver.createInputTopic(Constant.TOPIC_TWEEPS, new LongSerializer(), SerdeGenerator.<TweepAggregatedInformation>getSerde().serializer());
//
//        topicTwitterScore = driver.createOutputTopic(Constant.TOPIC_TWEET_SCORE, new LongDeserializer(), SerdeGenerator.<TwitterScore>getSerde().deserializer());
//        topicDlq = driver.createOutputTopic(Constant.TOPIC_SCORE_AGGREGATOR_DLQ, new VoidDeserializer(), new VoidDeserializer());
    }

    @AfterEach
    void close() {
        dlqProducer.close();
        driver.close();
    }

//    Status buildTweet(String text, Long userId) {
//        Status status = new Status();
//        status.setText(text);
//        status.setUser(User.newBuilder()
//                .setId(userId)
//                .setName("test user")
//                .setWithheldInCountries(Collections.emptyList())
//                .build());
//        status.setWithheldInCountries(Collections.singletonList("World"));
//        status.setContributors(Collections.emptyList());
//        return status;
//    }

    @Test
    void simpleUseCase() {
       assertTrue(true);
    }
}