package com.javahelps.wisdom.query.tree;

public abstract class Definition {

    private final String name;

    protected Definition(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
