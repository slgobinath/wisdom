package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.query.Query;

public class InsertIntoStatement implements Statement {

    private final String streamId;

    public InsertIntoStatement(String streamId) {
        this.streamId = streamId;
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        query.insertInto(this.streamId);
    }
}
