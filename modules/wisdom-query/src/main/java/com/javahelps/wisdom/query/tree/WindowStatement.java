package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.core.window.Window;

import java.util.ArrayList;
import java.util.HashMap;
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
        int count = 0;
        Map<String, Object> properties = new HashMap<>(this.keyValueElements.size());
        for (KeyValueElement element : this.keyValueElements) {
            String key = element.getKey();
            if (key == null) {
                key = String.format("_param_%d", count);
            }
            if (element.isVariable()) {
                properties.put(key, app.getVariable((String) element.getValue()));
            } else {
                properties.put(key, element.getValue());
            }
            count++;
        }
        Window window = Window.create(namespace, properties);
        query.window(window);
    }
}
