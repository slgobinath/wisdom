package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.AggregateOperator;
import com.javahelps.wisdom.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public class AggregateStatement implements Statement {

    private List<AggregateOperatorNode> operators = new ArrayList<>();


    public void addOperator(AggregateOperatorNode operator) {
        this.operators.add(operator);
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        int noOfOperators = operators.size();
        AggregateOperator[] aggregateOperators = new AggregateOperator[noOfOperators];
        for (int i = 0; i < noOfOperators; i++) {
            aggregateOperators[i] = this.operators.get(i).build(app, query);
        }
        query.aggregate(aggregateOperators);
    }
}
