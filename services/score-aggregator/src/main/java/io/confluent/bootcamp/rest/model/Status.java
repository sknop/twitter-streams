package io.confluent.bootcamp.rest.model;

public class Status {
    private int numberOfConsumer;
    private long summedOffset;
    private String state;

    public int getNumberOfConsumer() {
        return numberOfConsumer;
    }

    public void setNumberOfConsumer(int numberOfConsumer) {
        this.numberOfConsumer = numberOfConsumer;
    }

    public long getSummedOffset() {
        return summedOffset;
    }

    public void setSummedOffset(long summedOffset) {
        this.summedOffset = summedOffset;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
