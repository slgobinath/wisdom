package com.javahelps.wisdom.extensions.ml.onehot;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.operand.WisdomArray;
import com.javahelps.wisdom.core.util.Commons;

import java.util.*;
import java.util.stream.Collectors;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

@WisdomExtension("toOneHot")
public class ToOneHotMapper extends Mapper {

    private final String currentName;
    private final List<Comparable> items;
    private final int size;

    public ToOneHotMapper(String attrName, Map<String, ?> properties) {
        super(attrName, properties);
        this.currentName = Commons.getProperty(properties, ATTR, 0);
        Object items = Commons.getProperty(properties, "items", 1);
        if (this.currentName == null) {
            throw new WisdomAppValidationException("Required property %s for ToOneHot mapper not found", ATTR);
        }
        if (items instanceof List) {
            this.items = new ArrayList<>((List<Comparable>) items);
        } else if (items instanceof WisdomArray) {
            this.items = new ArrayList<>(((WisdomArray) items).toList());
        } else {
            throw new WisdomAppValidationException("items must be either java.util.List or WisdomArray");
        }
        if (this.items == null) {
            throw new WisdomAppValidationException("Required property items for ToOneHot mapper not found");
        }
        this.size = this.items.size();
        Collections.sort(this.items);
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
        int[] oneHot = new int[size];
        oneHot[items.indexOf(event.get(this.currentName))] = 1;
        List<Comparable> list = Arrays.stream(oneHot).boxed().collect(Collectors.toList());
        event.set(this.attrName, WisdomArray.of(list));
        return event;
    }
}
