package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Index;
import com.javahelps.wisdom.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public class SelectStatement implements Statement {

    private final List<String> attributes = new ArrayList<>();
    private final List<Integer> indices = new ArrayList<>();

    public void addAttribute(String attribute) {
        this.attributes.add(attribute);
    }

    public void addIndex(int index) {
        this.indices.add(index);
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        if (!this.attributes.isEmpty()) {
            query.select(attributes.toArray(new String[0]));
        } else if (!this.indices.isEmpty()) {
            final int noOfSize = indices.size();
            int[] values = new int[noOfSize];
            for (int i = 0; i < noOfSize; i++) {
                values[i] = indices.get(i);
            }
            query.select(Index.of(values));
        }
    }
}
