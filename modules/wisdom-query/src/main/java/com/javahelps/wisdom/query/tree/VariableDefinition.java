package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;

public class VariableDefinition extends Definition {

    private final Comparable value;

    public VariableDefinition(String name, Comparable value) {
        super(name);
        this.value = value;
    }

    public Comparable getValue() {
        return value;
    }

    @Override
    public void define(WisdomApp app) {
        app.defineVariable(this.getName(), this.value);
    }
}
