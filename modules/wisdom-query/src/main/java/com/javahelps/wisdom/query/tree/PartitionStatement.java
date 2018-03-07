package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public class PartitionStatement implements Statement {

    private List<String> attributes = new ArrayList<>();

    public void addAttribute(String attribute) {
        this.attributes.add(attribute);
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        query.partitionBy(this.attributes.toArray(new String[0]));
    }
}
