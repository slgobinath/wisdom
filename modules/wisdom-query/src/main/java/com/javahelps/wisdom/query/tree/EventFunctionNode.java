package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.function.event.EventFunction;
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.query.util.Utility;

import java.util.ArrayList;
import java.util.List;

public class EventFunctionNode implements OperatorElement {

    private String namespace;
    private List<KeyValueElement> keyValueElements = new ArrayList<>();

    public void addProperty(KeyValueElement element) {
        this.keyValueElements.add(element);
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public EventFunction build(WisdomApp app, Query query) {
        return EventFunction.create(namespace, Utility.toProperties(app, keyValueElements));
    }
}
