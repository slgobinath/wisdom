package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.variable.Variable;
import com.javahelps.wisdom.core.window.Window;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@WisdomExtension("unique:lengthBatch")
public class UniqueLengthBatchWindow extends Window implements Variable.OnUpdateListener<Integer> {

    private final String uniqueKey;
    private Map<Object, Event> eventMap;
    private int length;
    private Variable<Number> lengthVariable;

    public UniqueLengthBatchWindow(Map<String, ?> properties) {
        super(properties);
        Object uniqueKeyVal = this.getProperty("uniqueKey", 0);
        Object lengthVal = this.getProperty("length", 1);

        if (uniqueKeyVal instanceof String) {
            this.uniqueKey = (String) uniqueKeyVal;
        } else {
            throw new WisdomAppValidationException("uniqueKey of UniqueLengthBatchWindow must be java.lang.String but found %d", uniqueKeyVal.getClass().getSimpleName());
        }
        if (lengthVal instanceof Variable) {
            this.lengthVariable = (Variable<Number>) lengthVal;
            this.length = this.lengthVariable.get().intValue();
            this.lengthVariable.addOnUpdateListener(this);
        } else if (lengthVal instanceof Number) {
            this.length = ((Number) lengthVal).intValue();
        } else {
            throw new WisdomAppValidationException("length of UniqueLengthBatchWindow must be java.lang.Integer but found %s", lengthVal.getClass().getCanonicalName());
        }
        this.eventMap = new LinkedHashMap<>(this.length);
    }

    @Override
    public void process(Event event, Processor nextProcessor) {

        List<Event> eventsToSend = null;
        Object uniqueValue = event.get(this.uniqueKey);
        try {
            this.lock.lock();
            this.eventMap.put(uniqueValue, event);

            if (this.eventMap.size() >= length) {
                eventsToSend = new ArrayList<>(this.eventMap.values());
                this.eventMap.clear();
            }
        } finally {
            this.lock.unlock();
        }
        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }

    @Override
    public Window copy() {
        return new UniqueLengthBatchWindow(this.properties);
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.eventMap.clear();
        } finally {
            this.lock.unlock();
        }
    }


    @Override
    public void destroy() {
        if (this.lengthVariable != null) {
            this.lengthVariable.removeOnUpdateListener(this);
        }
        this.eventMap = null;
    }

    @Override
    public void update(Integer value) {
        try {
            this.lock.lock();
            this.length = value;
        } finally {
            this.lock.unlock();
        }
    }
}
