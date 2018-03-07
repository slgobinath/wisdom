package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.query.Query;

public class FilterStatement implements Statement {

    private final LogicalOperator operator;

    public FilterStatement(LogicalOperator operator) {
        this.operator = operator;
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        query.filter(this.operator.build(app, query));
    }
}
