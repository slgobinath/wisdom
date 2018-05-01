package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.util.Commons;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

@WisdomExtension("formatTime")
public class FormatTimeMapper extends Mapper {

    private final String currentName;
    private final ZoneId systemZone;

    public FormatTimeMapper(String attrName, Map<String, ?> properties) {
        super(attrName, properties);
        this.currentName = Commons.getProperty(properties, ATTR, 0);
        String zoneId = Commons.getProperty(properties, "zone", 1);
        if (this.currentName == null) {
            throw new WisdomAppValidationException("Required property %s for FormatTime mapper not found", ATTR);
        }
        if (zoneId == null) {
            systemZone = ZoneId.systemDefault();
        } else {
            systemZone = ZoneId.of(zoneId);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void init(WisdomApp wisdomApp) {

    }

    @Override
    public void stop() {

    }

    @Override
    public Event apply(Event event) {
        long timestamp = event.getAsLong(currentName);
        event.set(attrName, LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), systemZone).toString());
        return event;
    }
}
