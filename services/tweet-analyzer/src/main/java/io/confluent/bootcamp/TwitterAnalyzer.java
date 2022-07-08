package io.confluent.bootcamp;

import com.github.jcustenborder.kafka.connect.twitter.Status;
import io.confluent.bootcamp.rest.RestServer;
import io.confluent.bootcamp.sentiment.analyzer.TweetSentimentalAnalysis;
import io.confluent.bootcamp.streams.SerdeGenerator;
import io.confluent.bootcamp.streams.StreamApp;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Produced;


import java.util.Properties;

public class TwitterAnalyzer extends StreamApp {
    private TweetSentimentalAnalysis analysis = new TweetSentimentalAnalysis();

    public static void main(String[] args) throws Exception {
        TwitterAnalyzer streamApp = new TwitterAnalyzer();

        Properties extraProperties = new Properties();

        streamApp.run(args, extraProperties);streamApp.run(args, extraProperties);

        RestServer restServer = new RestServer();
        restServer.run();
    }

    @Override
    protected void buildTopology(StreamsBuilder builder) {
        builder.stream(Constant.TOPIC_TWEET, Consumed.with(Serdes.String(), SerdeGenerator.<Status>getSerde()))
                .map((k,v) -> new KeyValue<Long, TweetWithSentiment>(v.getUser().getId(),
                new TweetWithSentiment(v.getUser().getId(), v.getCreatedAt(), v.getText(), analysis.score(v.getText().toString()))))
                .to(Constant.TOPIC_TWEET_WITH_SENTIMENT, Produced.with(Serdes.Long(), SerdeGenerator.<TweetWithSentiment>getSerde()));
    }
}
