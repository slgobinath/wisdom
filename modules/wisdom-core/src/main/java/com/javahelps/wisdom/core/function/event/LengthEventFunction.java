package com.javahelps.wisdom.core.function.event;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.operand.WisdomArray;
import com.javahelps.wisdom.core.util.Commons;

import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

@WisdomExtension("len")
public class LengthEventFunction extends EventFunction {

    private final String attribute;

    public LengthEventFunction(Map<String, ?> properties) {
        super(properties);
        this.attribute = Commons.getProperty(properties, ATTR, 0);
        if (this.attribute == null) {
            throw new WisdomAppValidationException("Wisdom event:len expects a property %s but not found", ATTR);
        }
    }

    @Override
    public Object apply(Event event) {
        Object value = event.get(this.attribute);
        if (value instanceof WisdomArray) {
            return ((WisdomArray) value).size();
        } else {
            throw new WisdomAppRuntimeException("event:len expected WisdomArray but found %s", value.getClass().getCanonicalName());
        }
    }
}
