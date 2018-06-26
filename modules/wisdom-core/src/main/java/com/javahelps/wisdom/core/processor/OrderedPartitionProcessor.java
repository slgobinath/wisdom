package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;

import java.util.Objects;

public class OrderedPartitionProcessor extends PartitionProcessor {

    public OrderedPartitionProcessor(String id, String... attributes) {
        super(id, attributes);
    }

    protected String calculateKey(Event event) {
        if (this.attributes.length == 1) {
            return Objects.toString(event.get(this.attributes[0]));
        } else {
            StringBuilder builder = new StringBuilder();
            for (String attribute : this.attributes) {
                builder.append(Objects.toString(event.get(attribute)));
            }
            return builder.toString();
        }
    }

    @Override
    public Processor copy() {
        PartitionProcessor processor = new OrderedPartitionProcessor(this.id, this.attributes);
        processor.setNextProcessor(getNextProcessor().copy());
        return processor;
    }
}
