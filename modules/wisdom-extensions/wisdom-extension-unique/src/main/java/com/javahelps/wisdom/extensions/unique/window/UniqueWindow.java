package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.window.Window;

import java.time.Duration;

public abstract class UniqueWindow extends Window {

    public static Window externalTimeBatch(String uniqueKey, String timestampKey, Duration duration) {

        return new UniqueExternalTimeBatchWindow(uniqueKey, timestampKey, duration);
    }

    public static Window lengthBatch(String uniqueKey, int length) {

        return new UniqueLengthBatchWindow(uniqueKey, length);
    }

}
