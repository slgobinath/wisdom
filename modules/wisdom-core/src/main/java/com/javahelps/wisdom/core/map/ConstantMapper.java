package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.util.Commons;

import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.VALUE;

@WisdomExtension("constant")
public class ConstantMapper extends Mapper {

    private final Comparable constant;

    public ConstantMapper(String attrName, Map<String, ?> properties) {
        super(attrName, properties);
        this.constant = Commons.getProperty(properties, VALUE, 0);
        if (this.constant == null) {
            throw new WisdomAppValidationException("Required property %s for Constant mapper not found", VALUE);
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
        return event.set(this.attrName, this.constant);
    }
}
