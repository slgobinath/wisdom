package com.javahelps.wisdom.core.util;

/**
 * A {@link TimestampGenerator} which returns the system timestamp.
 */
public class SystemTimestampGenerator implements TimestampGenerator {

    public long currentTimestamp() {
        return System.currentTimeMillis();
    }
}
