package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.util.Commons;

import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

@WisdomExtension("int")
public class IntMapper extends Mapper {

    private final String currentName;

    public IntMapper(String attrName, Map<String, ?> properties) {
        super(attrName, properties);
        this.currentName = Commons.getProperty(properties, ATTR, 0);
        if (this.currentName == null) {
            throw new WisdomAppValidationException("Required property %s for Int mapper not found", ATTR);
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
    public Event map(Event event) {
        event.getData().put(attrName, event.getAsNumber(currentName).intValue());
//        Object value = event.get(currentName);
//        if (value == null) {
//            throw new AttributeNotFoundException(String.format("Attribute %s not found in event %s", currentName,
//                    this.toString()));
//        }
//        if (value instanceof Number) {
//            event.set(attrName, event.getAsNumber(currentName).intValue());
//            throw new WisdomAppRuntimeException(String.format("Cannot convert attribute %s from %s to Number", currentName, value.getClass().getSimpleName()));
//        } else if (value instanceof WisdomArray) {
//            event.set(attrName, ((WisdomArray) value).toIntArray());
//        }
        return event;
    }
}
