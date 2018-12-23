package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.query.Query;

import java.util.function.Predicate;

import static com.javahelps.wisdom.core.util.Commons.map;

public class LogicalOperator implements OperatorElement {


    private final String operator;
    private Object left;
    private Object right;

    public LogicalOperator(String operator) {
        this.operator = operator;
    }

    public void setLeft(Object left) {
        this.left = left;
    }

    public void setRight(Object right) {
        this.right = right;
    }


    public Predicate<Event> build(WisdomApp app, Query query) {
        if (this.operator == null) {
            return ((LogicalOperator) this.left).build(app, query);
        } else if ("not".equals(this.operator)) {
            return ((LogicalOperator) this.left).build(app, query).negate();
        } else if ("and".equals(this.operator)) {
            return ((LogicalOperator) this.left).build(app, query).and(((LogicalOperator) this.right).build(app, query));
        } else if ("or".equals(this.operator)) {
            return ((LogicalOperator) this.left).build(app, query).or(((LogicalOperator) this.right).build(app, query));
        } else {
            if (left instanceof VariableReference) {
                left = ((VariableReference) left).build(app);
            }
            if (right instanceof VariableReference) {
                right = ((VariableReference) right).build(app);
            }
            return com.javahelps.wisdom.core.operator.logical.LogicalOperator.create(this.operator, map("left", left, "right", right));
        }
    }
}
