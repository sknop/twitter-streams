package io.confluent.bootcamp;

import io.confluent.bootcamp.rest.RestServerWithStaticResource;
import io.confluent.bootcamp.streams.StreamApp;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class TwitterPart2 extends StreamApp {
    static Logger logger = LoggerFactory.getLogger(TwitterPart2.class.getName());

    public static void main(String[] args) throws Exception {
        Properties extraProperties = new Properties();
        TwitterPart2 streamApp = new TwitterPart2();
        streamApp.run(args, extraProperties);
        RestServerWithStaticResource restServerWithStaticResource = new RestServerWithStaticResource();
        restServerWithStaticResource.run();
    }

    @Override
    protected void buildTopology(StreamsBuilder builder) throws ExecutionException, InterruptedException {
        // Building all required state store
        // Building topology
    }
}
