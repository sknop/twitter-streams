package io.confluent.bootcamp;

import com.github.jcustenborder.kafka.connect.twitter.Status;
import io.confluent.bootcamp.rest.RestServer;
import io.confluent.bootcamp.streams.SerdeGenerator;
import io.confluent.bootcamp.streams.StreamApp;
import io.confluent.bootcamp.transformer.TweepAggregator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class TwitterPart1 extends StreamApp {
    static Logger logger = LoggerFactory.getLogger(TwitterPart1.class.getName());

    public static void main(String[] args) throws Exception {
        TwitterPart1 streamApp = new TwitterPart1();

        Properties extraProperties = new Properties();
        // To disable caching during the aggregation phase, otherwise some change could be hidden
        extraProperties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        streamApp.run(args, extraProperties);streamApp.run(args, extraProperties);

        RestServer restServer = new RestServer();
        restServer.run();
    }

    @Override
    protected void buildTopology(StreamsBuilder builder) {
        builder.stream(Constant.TOPIC_TWEET_WITH_SENTIMENT, Consumed.with(Serdes.Long(), SerdeGenerator.<TweetWithSentiment>getSerde()))
                .groupByKey().aggregate(TweepAggregate::new, new TweepAggregator(), Materialized
                        .<Long, TweepAggregate, KeyValueStore<Bytes, byte[]>>as(Constant.STORE_AGGREGATE_TWEEP)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(SerdeGenerator.getSerde()))
                .toStream()
                .filter((k, v) -> v.getDidChange())
                .to(Constant.TOPIC_TWEEPS, Produced.with(Serdes.Long(), SerdeGenerator.getSerde()));

    }
}
