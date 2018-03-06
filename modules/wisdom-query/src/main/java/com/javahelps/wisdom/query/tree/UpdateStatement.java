package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.query.Query;

public class UpdateStatement implements Statement {

    private final String variableId;

    public UpdateStatement(String variableId) {
        this.variableId = variableId;
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        query.update(this.variableId);
    }
}
