package com.javahelps.wisdom.service;

import com.javahelps.wisdom.core.window.Window;

import java.time.Duration;

public abstract class UniqueWindow extends Window {

    public static Window externalTimeBatch(String uniqueKey, String timestampKey, Duration duration) {

        return new UniqueExternalTimeBatchWindow(uniqueKey, timestampKey, duration);
    }

}
