package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.WisdomExtension;

import java.util.Map;

@WisdomExtension("rename")
public class RenameMapper extends Mapper {

    public RenameMapper(String currentName, String newName, Map<String, ?> properties) {
        super(currentName, newName, properties);
    }

    @Override
    public Event apply(Event event) {
        return event.rename(currentName, newName);
    }
}
