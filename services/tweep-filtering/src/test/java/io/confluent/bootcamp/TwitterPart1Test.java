package io.confluent.bootcamp;

import com.github.jcustenborder.kafka.connect.twitter.Status;
import com.github.jcustenborder.kafka.connect.twitter.User;
import io.confluent.bootcamp.streams.Context;
import io.confluent.bootcamp.streams.SerdeGenerator;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class TwitterPart1Test {
    private TopologyTestDriver driver;
    private TestInputTopic<String, Status> topicTweetStatus;
    private MockProducer<String, byte[]> dlqProducer;
    private TestOutputTopic<Long, TweepAggregate> topicTweeps;
    private TestOutputTopic<Void, Void> topicDlq;

    @BeforeEach
    void setup() {
        // Building mock client and schema registry
        dlqProducer = new MockProducer<>();
        Context.setProducer(dlqProducer);
        Context.getConfiguration().put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://local");

        // Inserting topology
        TwitterPart1 twitterPart1 = new TwitterPart1();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        twitterPart1.buildTopology(streamsBuilder);

        // Building topology test driver and topics
        driver = new TopologyTestDriver(streamsBuilder.build());
        topicTweetStatus = driver.createInputTopic(Constant.TOPIC_TWEET, new StringSerializer(), SerdeGenerator.<Status>getSerde().serializer());
        topicTweeps = driver.createOutputTopic(Constant.TOPIC_TWEEPS, new LongDeserializer(), SerdeGenerator.<TweepAggregate>getSerde().deserializer());
        topicDlq = driver.createOutputTopic(Constant.TOPIC_TWEEP_FILTERING_DLQ, new VoidDeserializer(), new VoidDeserializer());
    }

    @AfterEach
    void close() {
        dlqProducer.close();
        driver.close();
    }

    Status buildTweet(String text, Long userId, Instant creationTime) {
        Status status = new Status();
        status.setText(text);
        status.setUser(User.newBuilder()
                .setId(userId)
                .setName("test user")
                .setWithheldInCountries(Collections.emptyList())
                .build());

        status.setCreatedAt(creationTime);
        status.setWithheldInCountries(Collections.singletonList("World"));
        status.setContributors(Collections.emptyList());
        return status;
    }

    @Test
    void simpleCase() {
        Status tweet = buildTweet("Amazing! Awesome!", 1L, Instant.now());
        topicTweetStatus.pipeInput(tweet);

        assertTrue(topicTweeps.isEmpty());

        Status tweet2 = buildTweet("Amazing! Awesome!", 2L, Instant.now());
        topicTweetStatus.pipeInput(tweet2);

        assertTrue(topicTweeps.isEmpty());
    }

    @Test
    void shouldBlock() {
        Status tweet = buildTweet("Amazing! Awesome!", 1L, Instant.now());

        for (int i = 0; i < 5; i++ ) {
            topicTweetStatus.pipeInput(tweet, Instant.now());
            assertTrue(topicTweeps.isEmpty());
        }

        topicTweetStatus.pipeInput(tweet);
        assertFalse(topicTweeps.isEmpty());
        var tweepAggregateKV = topicTweeps.readKeyValue();
        assertTrue(tweepAggregateKV.value.getShouldBeBlocked());
        assertEquals(tweepAggregateKV.key, 1L);
        assertEquals(tweepAggregateKV.value.getUserId(),1L);
    }


    @Test
    void shouldNotBlockAfter24h() {
        Status tweet = buildTweet("Amazing! Awesome!", 1L, Instant.now());

        for (int i = 0; i < 5; i++ ) {
            topicTweetStatus.pipeInput(tweet, Instant.now());
            assertTrue(topicTweeps.isEmpty());
        }

        topicTweetStatus.pipeInput(tweet);
        assertFalse(topicTweeps.isEmpty());
        var tweepAggregatedInformation = topicTweeps.readValue();
        assertTrue(tweepAggregatedInformation.getShouldBeBlocked());

        tweet = buildTweet("Amazing! Awesome!", 1L, Instant.now().plus(25, ChronoUnit.HOURS));
        topicTweetStatus.pipeInput(tweet, Instant.now().plus(25, ChronoUnit.HOURS));
        assertFalse(topicTweeps.isEmpty());
        tweepAggregatedInformation = topicTweeps.readValue();
        assertFalse(tweepAggregatedInformation.getShouldBeBlocked());
    }
}