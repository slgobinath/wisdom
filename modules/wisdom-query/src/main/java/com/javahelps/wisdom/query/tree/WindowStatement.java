package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.core.window.Window;
import com.javahelps.wisdom.query.util.Utility;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WindowStatement implements Statement {

    private final String name;
    private String type;
    private List<KeyValueElement> keyValueElements = new ArrayList<>();

    public WindowStatement(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void addProperty(KeyValueElement element) {
        this.keyValueElements.add(element);
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        String namespace = type == null ? name : name + ":" + type;
        Map<String, Object> properties = Utility.toProperties(app, this.keyValueElements);
        Window window = Window.create(namespace, properties);
        query.window(window);
    }
}
