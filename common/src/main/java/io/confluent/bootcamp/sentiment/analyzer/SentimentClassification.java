package io.confluent.bootcamp.sentiment.analyzer;

public class SentimentClassification {
    int veryPositive;
    int positive;
    int neutral;
    int negative;
    int veryNegative;

    public int computeScore(String sentimentClass) {
        if (sentimentClass.equals("Very positive"))
            return getVeryPositive();
        if (sentimentClass.equals("Positive"))
            return getPositive();
        if (sentimentClass.equals("Neutral"))
            return getNeutral();
        if (sentimentClass.equals("Negative"))
            return getNegative();
        if (sentimentClass.equals("Very negative"))
            return getVeryNegative();

        throw new RuntimeException("Unexpected sentiment class " + sentimentClass);
    }

    public int getVeryPositive() {
        return veryPositive;
    }

    public void setVeryPositive(int veryPositive) {
        this.veryPositive = veryPositive;
    }

    public int getPositive() {
        return positive;
    }

    public void setPositive(int positive) {
        this.positive = positive;
    }

    public int getNeutral() {
        return neutral;
    }

    public void setNeutral(int neutral) {
        this.neutral = neutral;
    }

    public int getNegative() {
        return negative;
    }

    public void setNegative(int negative) {
        this.negative = negative;
    }

    public int getVeryNegative() {
        return veryNegative;
    }

    public void setVeryNegative(int veryNegative) {
        this.veryNegative = veryNegative;
    }
}
