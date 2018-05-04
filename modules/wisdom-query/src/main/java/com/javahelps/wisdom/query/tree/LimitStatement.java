package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.query.Query;

public class LimitStatement implements Statement {

    private final int[] bounds;

    public LimitStatement(int[] bounds) {
        this.bounds = bounds;
    }


    @Override
    public void addTo(WisdomApp app, Query query) {
        query.limit(this.bounds);
    }
}
