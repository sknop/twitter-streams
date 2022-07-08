package io.confluent.bootcamp.rest.model;

public class DashboardInformation {
    private Double averageMonthlyScore;
    private Double averageDailyScore;
    private Long numberOfTweetAnalyzed;
    private Long numberOfBlockedTweep;
    private TwitterScore lastScore;

    public DashboardInformation() {
    }

    public Double getAverageMonthlyScore() {
        return averageMonthlyScore;
    }

    public void setAverageMonthlyScore(Double averageMonthlyScore) {
        this.averageMonthlyScore = averageMonthlyScore;
    }

    public Double getAverageDailyScore() {
        return averageDailyScore;
    }

    public void setAverageDailyScore(Double averageDailyScore) {
        this.averageDailyScore = averageDailyScore;
    }

    public Long getNumberOfTweetAnalyzed() {
        return numberOfTweetAnalyzed;
    }

    public void setNumberOfTweetAnalyzed(Long numberOfTweetAnalyzed) {
        this.numberOfTweetAnalyzed = numberOfTweetAnalyzed;
    }

    public Long getNumberOfBlockedTweep() {
        return numberOfBlockedTweep;
    }

    public void setNumberOfBlockedTweep(Long numberOfBlockedTweep) {
        this.numberOfBlockedTweep = numberOfBlockedTweep;
    }

    public TwitterScore getLastScore() {
        return lastScore;
    }

    public void setLastScore(TwitterScore lastScore) {
        this.lastScore = lastScore;
    }
}
