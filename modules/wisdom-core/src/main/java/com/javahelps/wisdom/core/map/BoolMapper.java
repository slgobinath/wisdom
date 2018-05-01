package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.util.Commons;

import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

@WisdomExtension("bool")
public class BoolMapper extends Mapper {

    private final String currentName;

    public BoolMapper(String attrName, Map<String, ?> properties) {
        super(attrName, properties);
        this.currentName = Commons.getProperty(properties, ATTR, 0);
        if (this.currentName == null) {
            throw new WisdomAppValidationException("Required property %s for Bool mapper not found", ATTR);
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
        return event.set(attrName, event.getAsBool(currentName));
    }
}
