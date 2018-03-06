package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;

public abstract class Definition {

    private final String name;

    protected Definition(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public abstract void define(WisdomApp app);
}
