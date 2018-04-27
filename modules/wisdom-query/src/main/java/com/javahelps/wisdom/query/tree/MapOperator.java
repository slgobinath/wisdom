package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.query.util.Utility;

import java.util.ArrayList;
import java.util.List;

public class MapOperator implements OperatorElement {

    private String namespace;
    private String currentName;
    private String newName;
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

    public String getCurrentName() {
        return currentName;
    }

    public void setCurrentName(String currentName) {
        this.currentName = currentName;
    }

    public String getNewName() {
        return newName;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }

    public Mapper build(WisdomApp app, Query query) {
        return Mapper.create(namespace, currentName, newName, Utility.toProperties(app, keyValueElements));
    }
}
