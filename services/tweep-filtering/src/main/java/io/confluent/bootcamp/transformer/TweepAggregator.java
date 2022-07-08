package io.confluent.bootcamp.transformer;

import io.confluent.bootcamp.Constant;
import io.confluent.bootcamp.TweetWithSentiment;
import io.confluent.bootcamp.sentiment.analyzer.TweetSentimentalAnalysis;
import com.github.jcustenborder.kafka.connect.twitter.Status;
import io.confluent.bootcamp.TweepAggregate;
import io.confluent.bootcamp.streams.Context;
import org.apache.kafka.streams.kstream.Aggregator;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class TweepAggregator implements Aggregator<Long, TweetWithSentiment, TweepAggregate> {

    public TweepAggregator() {
    }

    @Override
    public TweepAggregate apply(Long key, TweetWithSentiment value, TweepAggregate aggregateResult) {
        try {
            return doApply(key, value, aggregateResult);
        } catch (Exception e) {
            try {
                Context.sendMessageToDLQ(Constant.TOPIC_TWEEP_FILTERING_DLQ, e, null);
                return null;
            } catch (Exception ex) {
                // If we can not send to the DLQ topic, let's fail and shutdown the application
                throw new RuntimeException(ex);
            }
        }
    }

    private TweepAggregate doApply(Long key, TweetWithSentiment value, TweepAggregate aggregate) {
        var previousShouldBeBlocked = aggregate.getShouldBeBlocked();
        if (aggregate.getUserId() != key) {
            aggregate.setUserId(key);
        }

        aggregate.setNumberOfTweet(aggregate.getNumberOfTweet() + 1);
        List<Long> lastTweets = aggregate.getLastTweetsTime();
        if (lastTweets == null) {
            lastTweets = new ArrayList<>();
        }
        lastTweets.add(value.getCreatedAt().toEpochMilli());

        if (lastTweets.size() > 6)
            lastTweets = lastTweets.subList(lastTweets.size() - 1, lastTweets.size());

        aggregate.setLastTweetsTime(lastTweets);
        aggregate.setNumberOfSentimentScore(aggregate.getNumberOfSentimentScore() + value.getSentiment());

        aggregate.setShouldBeBlocked(shouldBlock(aggregate, lastTweets));
        aggregate.setTime(Instant.now());
        // Checking if there is a change between the previous and the new update
        // Used to filter unchanged modification
        aggregate.setDidChange(previousShouldBeBlocked != aggregate.getShouldBeBlocked());

        return aggregate;
    }

    private boolean shouldBlock(TweepAggregate aggregate, List<Long> lastTweets) {
        var yesterdayTime = Instant.now().minus(24, ChronoUnit.HOURS).toEpochMilli();
        boolean didAllTheLastTweetsHappenedIn24Hours = true;
        for (Long lastTweetTime : lastTweets) {
            if (lastTweetTime < yesterdayTime) {
                didAllTheLastTweetsHappenedIn24Hours = false;
                break;
            }
        }

        if (lastTweets.size() > 5 && didAllTheLastTweetsHappenedIn24Hours) {
            double averageScore = aggregate.getNumberOfSentimentScore() / aggregate.getNumberOfTweet();
            return averageScore <= -1 || averageScore >= 1;
        } else {
            return false;
        }
    }
}
