package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.query.Query;

import java.util.function.Function;
import java.util.function.Predicate;

public class MathOperator implements OperatorElement {


    public enum Operation {
        SUM, AVG, MIN, MAX
    }

    private final Operation operation;
    private String as;
    private String attributeName;

    public MathOperator(Operation operation) {
        this.operation = operation;
    }

    public void setAs(String as) {
        this.as = as;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public Function<Event, Comparable> build(WisdomApp app, Query query) {
//        switch (this.operation) {
//            case AVG:
//                return Operator.AVG(this.attributeName)
//        }
        return null;
    }
}
