package com.javahelps.wisdom.core.time;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link TimestampGenerator} which returns the system timestamp.
 */
public class EventBasedTimestampGenerator implements TimestampGenerator {

    private long currentTimestamp = 0;
    private List<TimeChangeListener> timeChangeListeners = new ArrayList<>();


    public void addTimeChangeListener(TimeChangeListener listener) {
        this.timeChangeListeners.add(listener);
    }

    public void removeTimeChangeListener(TimeChangeListener listener) {
        this.timeChangeListeners.remove(listener);
    }

    public void setCurrentTimestamp(long currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
        for (TimeChangeListener listener : this.timeChangeListeners) {
            listener.onTimeChange(currentTimestamp);
        }
    }

    public long currentTimestamp() {
        return this.currentTimestamp;
    }

    public interface TimeChangeListener {
        void onTimeChange(long timestamp);
    }
}
