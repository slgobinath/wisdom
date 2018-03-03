package com.javahelps.wisdom.query.tree;

public class VariableDefinition extends Definition {

    private Comparable value;

    public VariableDefinition(String name) {
        super(name);
    }

    public Comparable getValue() {
        return value;
    }

    public void setValue(Comparable value) {
        this.value = value;
    }
}
