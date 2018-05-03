package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.variable.Variable;

public class VariableReference {

    private final String variableId;

    public VariableReference(String variableId) {
        this.variableId = variableId;
    }

    public String getVariableId() {
        return variableId;
    }

    public Variable<Comparable> build(WisdomApp app) {
        return app.getVariable(this.variableId);
    }
}
