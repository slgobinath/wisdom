package com.javahelps.wisdom.query.tree;

public class KeyValueElement {

    private String key;
    private Object value;

    public static KeyValueElement of(String key, Comparable value) {
        KeyValueElement element = new KeyValueElement();
        element.setKey(key);
        element.setValue(value);
        return element;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
