package com.javahelps.wisdom.dev.client;

import java.io.IOException;
import java.util.Map;

public abstract class WisdomClient implements AutoCloseable {

    public abstract Response send(String streamId, Map<String, Comparable> data) throws IOException;

    @Override
    public abstract void close() throws IOException;
}
