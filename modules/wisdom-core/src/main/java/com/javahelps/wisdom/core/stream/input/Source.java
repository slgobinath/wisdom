package com.javahelps.wisdom.core.stream.input;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;

import java.util.Map;

public abstract class Source {

    public Source(Map<String, ?> properties) {

    }

    public static Source create(String namespace, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createSource(namespace, properties);
    }

    public abstract void init(WisdomApp wisdomApp, String streamId);

    public abstract void start();

    public abstract void stop();
}
