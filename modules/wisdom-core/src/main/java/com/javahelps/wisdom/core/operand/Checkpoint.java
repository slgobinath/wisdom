package com.javahelps.wisdom.core.operand;

import java.util.HashMap;
import java.util.Map;

public class Checkpoint {

    private static final Map<String, Checkpoint> CHECKPOINTS = new HashMap<>();
    private final String id;
    private final Map<String, Object> map = new HashMap<>();

    private Checkpoint(String id) {
        this.id = id;
    }

    public static Checkpoint forID(String id) {
        Checkpoint checkpoint = CHECKPOINTS.get(id);
        if (checkpoint == null) {
            checkpoint = new Checkpoint(id);
            CHECKPOINTS.put(id, checkpoint);
        }
        return checkpoint;
    }

    public <T> void add(String key, T value) {
        map.put(key, value);
    }

    public <T> T get(String key) {
        return (T) this.map.get(key);
    }

    public String getID() {
        return this.id;
    }
}
