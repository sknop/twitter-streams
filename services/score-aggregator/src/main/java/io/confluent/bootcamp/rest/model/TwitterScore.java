package io.confluent.bootcamp.rest.model;

import java.time.Instant;

public class TwitterScore {

    private double score;
    private int numberOfPartitionReceived;
    private long numberOfTweets;
    private Instant time;

    public TwitterScore() {
    }

//    public TwitterScore(io.confluent.bootcamp.TwitterScore c) {
//        score = c.getScore();
//        numberOfPartitionReceived = c.getNumberOfPartitionReceived();
//        numberOfTweets = c.getNumberOfTweets();
//        time = c.getTime();
//    }

    public double getScore() {
        return score;
    }

    public int getNumberOfPartitionReceived() {
        return numberOfPartitionReceived;
    }

    public long getNumberOfTweets() {
        return numberOfTweets;
    }

    public Instant getTime() {
        return time;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public void setNumberOfPartitionReceived(int numberOfPartitionReceived) {
        this.numberOfPartitionReceived = numberOfPartitionReceived;
    }

    public void setNumberOfTweets(long numberOfTweets) {
        this.numberOfTweets = numberOfTweets;
    }

    public void setTime(Instant time) {
        this.time = time;
    }
}
