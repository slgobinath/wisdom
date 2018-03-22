package com.javahelps.wisdom.query.tree;

public class KeyValueElement {

    private String key;
    private Comparable value;
    private boolean isVariable;

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public Comparable getValue() {
        return value;
    }

    public void setValue(Comparable value) {
        this.value = value;
    }

    public void setVariable(boolean variable) {
        isVariable = variable;
    }

    public boolean isVariable() {
        return isVariable;
    }
}