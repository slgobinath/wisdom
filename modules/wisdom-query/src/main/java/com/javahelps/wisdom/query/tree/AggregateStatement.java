package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.AggregateOperator;
import com.javahelps.wisdom.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public class AggregateStatement implements Statement {

    private List<AggregateOperator> operators = new ArrayList<>();


    public void addOperator(AggregateOperator operator) {
        this.operators.add(operator);
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        query.aggregate(this.operators.toArray(new AggregateOperator[0]));
    }
}
