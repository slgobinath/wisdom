package com.javahelps.wisdom.query.tree;

public class VariableDefinition extends Definition {

    private final Comparable value;

    public VariableDefinition(String name, Comparable value) {
        super(name);
        this.value = value;
    }

    public Comparable getValue() {
        return value;
    }
}
