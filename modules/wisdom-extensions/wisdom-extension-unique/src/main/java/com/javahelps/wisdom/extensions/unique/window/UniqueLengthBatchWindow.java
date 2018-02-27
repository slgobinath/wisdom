package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.window.Window;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UniqueLengthBatchWindow extends UniqueWindow {

    private final Map<Comparable, Event> eventMap = new LinkedHashMap<>();
    private final String uniqueKey;
    private int length;

    public UniqueLengthBatchWindow(String uniqueKey, int length) {

        this.uniqueKey = uniqueKey;
        this.length = length;
    }

    @Override
    public void process(Event event, Processor nextProcessor) {

        List<Event> eventsToSend = null;
        Comparable uniqueValue = event.get(this.uniqueKey);
        synchronized (this) {
            this.eventMap.put(uniqueValue, event);

            if (this.eventMap.size() >= length) {
                eventsToSend = new ArrayList<>(this.eventMap.values());
                this.eventMap.clear();
            }
        }
        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }

    @Override
    public Window copy() {

        Window window = new UniqueLengthBatchWindow(this.uniqueKey, this.length);
        return window;
    }
}
