package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.util.Commons;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

@WisdomExtension("formatTime")
public class FormatTimeMapper extends Mapper {

    private final ZoneId systemZone;

    public FormatTimeMapper(String currentName, String newName, Map<String, ?> properties) {
        super(currentName, newName, properties);
        String zoneId = Commons.getProperty(properties, "zone", 0);
        if (zoneId == null) {
            systemZone = ZoneId.systemDefault();
        } else {
            systemZone = ZoneId.of(zoneId);
        }
    }

    public Comparable map(Comparable value) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value), systemZone).toString();
    }

    @Override
    public Event apply(Event event) {
        long timestamp = event.getAsLong(currentName);
        event.set(newName, LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), systemZone).toString());
        return event;
    }
}
