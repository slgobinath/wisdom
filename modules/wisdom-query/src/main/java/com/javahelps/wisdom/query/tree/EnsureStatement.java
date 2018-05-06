package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.query.Query;

public class EnsureStatement implements Statement {

    private final int[] bounds;

    public EnsureStatement(int[] bounds) {
        this.bounds = bounds;
    }


    @Override
    public void addTo(WisdomApp app, Query query) {
        query.ensure(this.bounds);
    }
}
