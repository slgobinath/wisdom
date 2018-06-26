package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;

import java.util.Objects;

public class UnorderedPartitionProcessor extends PartitionProcessor {

    public UnorderedPartitionProcessor(String id, String... attributes) {
        super(id, attributes);
    }

    protected String calculateKey(Event event) {
        if (this.attributes.length == 1) {
            return Objects.toString(event.get(this.attributes[0]));
        } else {
            long hash = 0;
            for (String attribute : this.attributes) {
                hash += Objects.toString(event.get(attribute)).hashCode();
            }
            return Long.toString(hash);
        }
    }

    @Override
    public Processor copy() {
        PartitionProcessor processor = new UnorderedPartitionProcessor(this.id, this.attributes);
        processor.setNextProcessor(getNextProcessor().copy());
        return processor;
    }
}
