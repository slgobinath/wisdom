package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.query.Query;

import java.util.Objects;
import java.util.function.Predicate;

public class LogicalOperator implements OperatorElement {


    public enum Operation {
        GT, GT_EQ, LT, LT_EQ, AND, OR, NOT, EQ, IDENTICAL
    }

    private final Operation operation;
    private String leftVar;
    private String rightVar;
    private Comparable leftComparable;
    private Comparable rightComparable;
    private String leftAttr;
    private String rightAttr;
    private LogicalOperator leftOperator;
    private LogicalOperator rightOperator;

    public LogicalOperator(Operation operation) {
        this.operation = operation;
    }


    public void setLeftVar(String leftVar) {
        this.leftVar = leftVar;
    }

    public void setRightVar(String rightVar) {
        this.rightVar = rightVar;
    }

    public void setLeftComparable(Comparable leftComparable) {
        this.leftComparable = leftComparable;
    }

    public void setRightComparable(Comparable rightComparable) {
        this.rightComparable = rightComparable;
    }

    public void setLeftAttr(String leftAttr) {
        this.leftAttr = leftAttr;
    }

    public void setRightAttr(String rightAttr) {
        this.rightAttr = rightAttr;
    }

    public void setLeftOperator(LogicalOperator leftOperator) {
        this.leftOperator = leftOperator;
    }

    public void setRightOperator(LogicalOperator rightOperator) {
        this.rightOperator = rightOperator;
    }

    private Predicate<Event> greaterThan(WisdomApp app) {
        Predicate<Event> predicate = null;
        if (leftAttr != null) {
            if (rightAttr != null) {
                // attr > attr
                predicate = Operator.GREATER_THAN(leftAttr, rightAttr);
            } else if (rightVar != null) {
                // attr > $var
                predicate = Operator.GREATER_THAN(leftAttr, app.getVariable(rightVar));
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // attr > 10
                predicate = Operator.GREATER_THAN(leftAttr, ((Number) rightComparable).doubleValue());
            }
        } else if (leftVar != null) {
            if (rightAttr != null) {
                // $var > attr
                predicate = Operator.LESS_THAN(rightAttr, app.getVariable(leftVar));
            } else if (rightVar != null) {
                // $var > $var
                predicate = Operator.GREATER_THAN(app.getVariable(leftVar), app.getVariable(rightVar));
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // $var > 10
                predicate = Operator.GREATER_THAN(app.getVariable(leftVar), ((Number) rightComparable).doubleValue());
            }
        } else if (leftComparable != null && leftComparable instanceof Number) {
            if (rightAttr != null) {
                // 10 > attr
                predicate = Operator.LESS_THAN(rightAttr, ((Number) leftComparable).doubleValue());
            } else if (rightVar != null) {
                // 10 > $var
                predicate = Operator.LESS_THAN(app.getVariable(rightVar), ((Number) leftComparable).doubleValue());
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // $var > 10
                final boolean result = ((Number) leftComparable).doubleValue() > ((Number) rightComparable).doubleValue();
                predicate = event -> result;
            }
        }
        return predicate;
    }

    private Predicate<Event> greaterThanOrEqual(WisdomApp app) {
        Predicate<Event> predicate = null;
        if (leftAttr != null) {
            if (rightAttr != null) {
                // attr > attr
                predicate = Operator.GREATER_THAN_OR_EQUAL(leftAttr, rightAttr);
            } else if (rightVar != null) {
                // attr > $var
                predicate = Operator.GREATER_THAN_OR_EQUAL(leftAttr, app.getVariable(rightVar));
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // attr > 10
                predicate = Operator.GREATER_THAN_OR_EQUAL(leftAttr, ((Number) rightComparable).doubleValue());
            }
        } else if (leftVar != null) {
            if (rightAttr != null) {
                // $var > attr
                predicate = Operator.LESS_THAN_OR_EQUAL(rightAttr, app.getVariable(leftVar));
            } else if (rightVar != null) {
                // $var > $var
                predicate = Operator.GREATER_THAN_OR_EQUAL(app.getVariable(leftVar), app.getVariable(rightVar));
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // $var > 10
                predicate = Operator.GREATER_THAN_OR_EQUAL(app.getVariable(leftVar), ((Number) rightComparable).doubleValue());
            }
        } else if (leftComparable != null && leftComparable instanceof Number) {
            if (rightAttr != null) {
                // 10 > attr
                predicate = Operator.LESS_THAN_OR_EQUAL(rightAttr, ((Number) leftComparable).doubleValue());
            } else if (rightVar != null) {
                // 10 > $var
                predicate = Operator.LESS_THAN_OR_EQUAL(app.getVariable(rightVar), ((Number) leftComparable).doubleValue());
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // $var > 10
                final boolean result = ((Number) leftComparable).doubleValue() >= ((Number) rightComparable).doubleValue();
                predicate = event -> result;
            }
        }
        return predicate;
    }

    private Predicate<Event> lessThan(WisdomApp app) {
        Predicate<Event> predicate = null;
        if (leftAttr != null) {
            if (rightAttr != null) {
                // attr < attr
                predicate = Operator.LESS_THAN(leftAttr, rightAttr);
            } else if (rightVar != null) {
                // attr < $var
                predicate = Operator.LESS_THAN(leftAttr, app.getVariable(rightVar));
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // attr < 10
                predicate = Operator.LESS_THAN(leftAttr, ((Number) rightComparable).doubleValue());
            }
        } else if (leftVar != null) {
            if (rightAttr != null) {
                // $var < attr
                predicate = Operator.GREATER_THAN(rightAttr, app.getVariable(leftVar));
            } else if (rightVar != null) {
                // $var < $var
                predicate = Operator.LESS_THAN(app.getVariable(leftVar), app.getVariable(rightVar));
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // $var < 10
                predicate = Operator.LESS_THAN(app.getVariable(leftVar), ((Number) rightComparable).doubleValue());
            }
        } else if (leftComparable != null && leftComparable instanceof Number) {
            if (rightAttr != null) {
                // 10 < attr
                predicate = Operator.GREATER_THAN(rightAttr, ((Number) leftComparable).doubleValue());
            } else if (rightVar != null) {
                // 10 < $var
                predicate = Operator.GREATER_THAN(app.getVariable(rightVar), ((Number) leftComparable).doubleValue());
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // $var < 10
                final boolean result = ((Number) leftComparable).doubleValue() < ((Number) rightComparable).doubleValue();
                predicate = event -> result;
            }
        }
        return predicate;
    }

    private Predicate<Event> lessThanOrEqual(WisdomApp app) {
        Predicate<Event> predicate = null;
        if (leftAttr != null) {
            if (rightAttr != null) {
                // attr < attr
                predicate = Operator.LESS_THAN_OR_EQUAL(leftAttr, rightAttr);
            } else if (rightVar != null) {
                // attr < $var
                predicate = Operator.LESS_THAN_OR_EQUAL(leftAttr, app.getVariable(rightVar));
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // attr < 10
                predicate = Operator.LESS_THAN_OR_EQUAL(leftAttr, ((Number) rightComparable).doubleValue());
            }
        } else if (leftVar != null) {
            if (rightAttr != null) {
                // $var < attr
                predicate = Operator.GREATER_THAN_OR_EQUAL(rightAttr, app.getVariable(leftVar));
            } else if (rightVar != null) {
                // $var < $var
                predicate = Operator.LESS_THAN_OR_EQUAL(app.getVariable(leftVar), app.getVariable(rightVar));
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // $var < 10
                predicate = Operator.LESS_THAN_OR_EQUAL(app.getVariable(leftVar), ((Number) rightComparable).doubleValue());
            }
        } else if (leftComparable != null && leftComparable instanceof Number) {
            if (rightAttr != null) {
                // 10 < attr
                predicate = Operator.GREATER_THAN_OR_EQUAL(rightAttr, ((Number) leftComparable).doubleValue());
            } else if (rightVar != null) {
                // 10 < $var
                predicate = Operator.GREATER_THAN_OR_EQUAL(app.getVariable(rightVar), ((Number) leftComparable).doubleValue());
            } else if (rightComparable != null && rightComparable instanceof Number) {
                // $var < 10
                final boolean result = ((Number) leftComparable).doubleValue() <= ((Number) rightComparable)
                        .doubleValue();
                predicate = event -> result;
            }
        }
        return predicate;
    }

    private Predicate<Event> equals(WisdomApp app) {
        Predicate<Event> predicate = null;
        if (leftAttr != null) {
            if (rightAttr != null) {
                // attr == attr
                predicate = Operator.EQUAL_ATTRIBUTES(leftAttr, rightAttr);
            } else if (rightVar != null) {
                // attr == $var
                predicate = Operator.EQUALS(leftAttr, app.getVariable(rightVar));
            } else if (rightComparable != null) {
                // attr == 10
                predicate = Operator.EQUALS(leftAttr, rightComparable);
            }
        } else if (leftVar != null) {
            if (rightAttr != null) {
                // $var == attr
                predicate = Operator.EQUALS(rightAttr, app.getVariable(leftVar));
            } else if (rightVar != null) {
                // $var == $var
                predicate = Operator.EQUALS(app.getVariable(leftVar), app.getVariable(rightVar));
            } else if (rightComparable != null) {
                // $var == 10
                predicate = Operator.EQUALS(app.getVariable(leftVar), ((Number) rightComparable).doubleValue());
            }
        } else if (leftComparable != null) {
            if (rightAttr != null) {
                // 10 == attr
                predicate = Operator.EQUALS(rightAttr, leftComparable);
            } else if (rightVar != null) {
                // 10 == $var
                predicate = Operator.EQUALS(app.getVariable(rightVar), leftComparable);
            } else if (rightComparable != null) {
                // $var == 10
                final boolean result = Objects.equals(leftComparable, rightComparable);
                predicate = event -> result;
            }
        }
        return predicate;
    }

    public Predicate<Event> build(WisdomApp app, Query query) {
        switch (this.operation) {
            case IDENTICAL:
                return this.leftOperator.build(app, query);
            case NOT:
                return this.leftOperator.build(app, query).negate();
            case AND:
                return this.leftOperator.build(app, query).and(this.rightOperator.build(app, query));
            case OR:
                return this.leftOperator.build(app, query).or(this.rightOperator.build(app, query));
            case EQ:
                return this.equals(app);
            case GT:
                return this.greaterThan(app);
            case GT_EQ:
                return this.greaterThanOrEqual(app);
            case LT:
                return this.lessThan(app);
            case LT_EQ:
                return this.lessThanOrEqual(app);
        }
        return null;
    }
}
