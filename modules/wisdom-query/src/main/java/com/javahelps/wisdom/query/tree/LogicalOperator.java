package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.query.Query;

import java.util.Map;
import java.util.function.Predicate;

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
            return com.javahelps.wisdom.core.operator.logical.LogicalOperator.create(this.operator, Map.of("left", left, "right", right));
        }
//        switch (this.operation) {
//            case IDENTICAL:
//                return this.leftOperator.build(app, query);
//            case NOT:
//                return this.leftOperator.build(app, query).negate();
//            case AND:
//                return this.leftOperator.build(app, query).and(this.rightOperator.build(app, query));
//            case OR:
//                return this.leftOperator.build(app, query).or(this.rightOperator.build(app, query));
//            case EQ:
//                return this.equals(app);
//            case GT:
//                return this.greaterThan(app);
//            case GT_EQ:
//                return this.greaterThanOrEqual(app);
//            case LT:
//                return this.lessThan(app);
//            case LT_EQ:
//                return this.lessThanOrEqual(app);
//            case IN:
//                return this.in(app);
//
//        }
    }
}
