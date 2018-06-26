package com.javahelps.wisdom.query.tree;

import java.util.ArrayList;
import java.util.List;

public abstract class PartitionStatement implements Statement {

    protected List<String> attributes = new ArrayList<>();

    public void addAttribute(String attribute) {
        this.attributes.add(attribute);
    }
}
