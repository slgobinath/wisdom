package com.javahelps.wisdom.core.statistics;

import java.util.concurrent.atomic.AtomicLong;

public class StreamTracker {

    private final String streamId;
    private final AtomicLong count = new AtomicLong();
    private long startTime;

    public StreamTracker(String streamId) {
        this.streamId = streamId;
    }

    public void inEvent() {
        this.inEvent(1L);
    }

    public void inEvent(long count) {
        this.count.addAndGet(count);
    }

    void reset() {
        this.count.set(0L);
    }

    long getStartTime() {
        return startTime;
    }

    void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getStreamId() {
        return streamId;
    }

    public long getCount() {
        return this.count.get();
    }
}
