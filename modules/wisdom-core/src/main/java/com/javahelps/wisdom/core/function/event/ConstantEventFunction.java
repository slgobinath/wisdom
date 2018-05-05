package com.javahelps.wisdom.core.function.event;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.util.Commons;

import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.VALUE;

@WisdomExtension("constant")
public class ConstantEventFunction extends EventFunction {

    private final Object value;

    public ConstantEventFunction(Map<String, ?> properties) {
        super(properties);
        this.value = Commons.getProperty(properties, VALUE, 0);
        if (this.value == null) {
            throw new WisdomAppValidationException("Wisdom event function 'constant' expects a property %s but not found", VALUE);
        }
    }

    @Override
    public Object apply(Event event) {
        return value;
    }
}
