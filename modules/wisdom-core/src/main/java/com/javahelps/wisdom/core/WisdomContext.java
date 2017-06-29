package com.javahelps.wisdom.core;

import com.javahelps.wisdom.core.util.SystemTimestampGenerator;
import com.javahelps.wisdom.core.util.TimestampGenerator;

/**
 * The user defined and system defined configurations of the stream processor are stored in {@link WisdomContext}.
 */
public class WisdomContext {
    private TimestampGenerator timestampGenerator;

    public WisdomContext() {
        this.timestampGenerator = new SystemTimestampGenerator();
    }

    public TimestampGenerator getTimestampGenerator() {
        return timestampGenerator;
    }
}
