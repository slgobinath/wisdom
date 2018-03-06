package com.javahelps.wisdom.query.antlr;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.query.antlr4.WisdomQLBaseVisitor;
import com.javahelps.wisdom.query.antlr4.WisdomQLParser;
import com.javahelps.wisdom.query.tree.*;
import com.javahelps.wisdom.query.util.Utility;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.function.Predicate;

import static com.javahelps.wisdom.query.util.Constants.ANNOTATION.*;

public class WisdomQLBaseVisitorImpl extends WisdomQLBaseVisitor {

    @Override
    public WisdomApp visitWisdom_app(WisdomQLParser.Wisdom_appContext ctx) {
        WisdomApp wisdomApp;
        // Create WisdomApp
        if (ctx.annotation() != null) {
            Annotation annotation = (Annotation) visit(ctx.annotation());
            Utility.verifyAnnotation(ctx.annotation(), annotation, APP_ANNOTATION, NAME, VERSION);
            wisdomApp = new WisdomApp(annotation.getProperty(NAME), annotation.getProperty(VERSION));
        } else {
            wisdomApp = new WisdomApp();
        }
        // Define components
        for (ParseTree tree : ctx.definition()) {
            Definition definition = (Definition) visit(tree);
            if (definition instanceof StreamDefinition) {
                wisdomApp.defineStream(definition.getName());
            } else if (definition instanceof VariableDefinition) {
                VariableDefinition varDef = (VariableDefinition) definition;
                wisdomApp.defineVariable(varDef.getName(), varDef.getValue());
            }
        }
        // Create queries
        for (ParseTree tree : ctx.query()) {
            QueryNode queryNode = (QueryNode) visit(tree);
            Query query = wisdomApp.defineQuery(queryNode.getName());
            queryNode.build(query);
        }

        return wisdomApp;
    }

    @Override
    public Definition visitDefinition(WisdomQLParser.DefinitionContext ctx) {
        if (ctx.def_stream() != null) {
            return (Definition) visit(ctx.def_stream());
        } else if (ctx.def_variable() != null) {
            return (Definition) visit(ctx.def_variable());
        } else {
            throw new WisdomParserException(ctx, "unknown definition");
        }
    }

    @Override
    public VariableDefinition visitDef_variable(WisdomQLParser.Def_variableContext ctx) {
        return new VariableDefinition(ctx.NAME().getText(), (Comparable) visit(ctx.wisdom_primitive()));
    }

    @Override
    public StreamDefinition visitDef_stream(WisdomQLParser.Def_streamContext ctx) {
        return new StreamDefinition(ctx.NAME().getText());
    }

    @Override
    public Annotation visitAnnotation(WisdomQLParser.AnnotationContext ctx) {
        Annotation annotation = new Annotation(ctx.NAME().getText());
        for (ParseTree tree : ctx.annotation_element()) {
            AnnotationElement element = (AnnotationElement) visit(tree);
            annotation.setProperty(element.getKey(), element.getValue());
        }
        return annotation;
    }

    @Override
    public AnnotationElement visitAnnotation_element(WisdomQLParser.Annotation_elementContext ctx) {
        AnnotationElement element = new AnnotationElement();
        if (ctx.NAME() != null) {
            element.setKey(ctx.NAME().getText());
        }
        if (ctx.wisdom_primitive() != null) {
            element.setValue((Comparable) visit(ctx.wisdom_primitive()));
        }
        return element;
    }

    @Override
    public KeyValueElement visitOptional_key_value_element(WisdomQLParser.Optional_key_value_elementContext ctx) {
        KeyValueElement element = new KeyValueElement();
        if (ctx.NAME() != null) {
            element.setKey(ctx.NAME().getText());
        }
        element.setValue((Comparable) visit(ctx.wisdom_primitive()));
        return element;
    }

    @Override
    public Statement visitSelect_statement(WisdomQLParser.Select_statementContext ctx) {
        SelectStatement selectStatement = new SelectStatement();
        if (ctx.STAR() == null) {
            for (TerminalNode attribute : ctx.NAME()) {
                selectStatement.addAttribute(attribute.getText());
            }
        }
        return selectStatement;
    }

    @Override
    public Statement visitFilter_statement(WisdomQLParser.Filter_statementContext ctx) {
        return new FilterStatement((Predicate<Event>) visit(ctx.logical_operator()));
    }

    @Override
    public Statement visitWindow_statement(WisdomQLParser.Window_statementContext ctx) {
        WindowStatement statement = new WindowStatement(ctx.name.getText());
        if (ctx.type != null) {
            statement.setType(ctx.type.getText());
        }
        for (ParseTree tree : ctx.optional_key_value_element()) {
            KeyValueElement element = (KeyValueElement) visit(tree);
            statement.addProperty(element.getKey(), element.getValue());
        }
        return statement;
    }

    @Override
    public Statement visitQuery_statement(WisdomQLParser.Query_statementContext ctx) {
        if (ctx.select_statement() != null) {
            return (Statement) visit(ctx.select_statement());
        } else if (ctx.filter_statement() != null) {
            return (Statement) visit(ctx.filter_statement());
        } else if (ctx.window_statement() != null) {
            return (Statement) visit(ctx.window_statement());
        } else {
            throw new WisdomParserException(ctx, "unknown query statement");
        }
    }

    @Override
    public QueryNode visitQuery(WisdomQLParser.QueryContext ctx) {
        QueryNode queryNode = new QueryNode(ctx.input.getText(), ctx.output.getText());
        if (ctx.annotation() != null) {
            Annotation annotation = (Annotation) visit(ctx.annotation());
            Utility.verifyAnnotation(ctx.annotation(), annotation, QUERY_ANNOTATION, NAME);
            queryNode.setName(annotation.getProperty(NAME));
        }
        for (ParseTree tree : ctx.query_statement()) {
            queryNode.addStatement((Statement) visit(tree));
        }
        return queryNode;
    }

    @Override
    public Predicate<Event> visitLogical_operator(WisdomQLParser.Logical_operatorContext ctx) {
        Predicate<Event> predicate = null;
        int noOfLogicalOperators = ctx.logical_operator().size();
        if (noOfLogicalOperators == 1) {
            predicate = (Predicate<Event>) visit(ctx.logical_operator(0));
            if (ctx.NOT() != null) {
                predicate = predicate.negate();
            }
        } else if (noOfLogicalOperators == 2) {
            Predicate<Event> left = (Predicate<Event>) visit(ctx.logical_operator(0));
            Predicate<Event> right = (Predicate<Event>) visit(ctx.logical_operator(1));
            if (ctx.AND() != null) {
                predicate = left.and(right);
            } else if (ctx.OR() != null) {
                predicate = left.or(right);
            }
        } else if (noOfLogicalOperators == 0) {
            if (ctx.GREATER_THAN() != null) {
                if (ctx.NAME().size() == 2) {
                    predicate = Operator.GREATER_THAN(ctx.left.getText(), ctx.right.getText());
                } else {
                    if (ctx.right.getType() == ctx.NUMBER().getSymbol().getType()) {
                        predicate = Operator.GREATER_THAN(ctx.left.getText(), Double.parseDouble(ctx.right.getText()));
                    } else if (ctx.left.getType() == ctx.NUMBER().getSymbol().getType()) {
                        predicate = Operator.LESS_THAN(ctx.right.getText(), Double.parseDouble(ctx.left.getText()));
                    }
                }
            } else if (ctx.LESS_THAN() != null) {
                if (ctx.NAME().size() == 2) {
                    predicate = Operator.LESS_THAN(ctx.left.getText(), ctx.right.getText());
                } else {
                    if (ctx.right.getType() == ctx.NUMBER().getSymbol().getType()) {
                        predicate = Operator.LESS_THAN(ctx.left.getText(), Double.parseDouble(ctx.right.getText()));
                    } else if (ctx.left.getType() == ctx.NUMBER().getSymbol().getType()) {
                        predicate = Operator.GREATER_THAN(ctx.right.getText(), Double.parseDouble(ctx.left.getText()));
                    }
                }
            } else if (ctx.EQUALS() != null) {
                if (ctx.NAME().size() == 2) {
                    predicate = Operator.EQUAL_ATTRIBUTES(ctx.NAME(0).getText(), ctx.NAME(1).getText());
                } else {
                    if (ctx.wisdom_primitive() != null) {
                        predicate = Operator.EQUALS(ctx.NAME(0).getText(), (Comparable) visit(ctx.wisdom_primitive()));
                    } else {
                        throw new WisdomParserException(ctx, "unknown operand for equality");
                    }
                }
            }
        }
        if (predicate == null) {
            throw new WisdomParserException(ctx, "unknown logical operator");
        }
        return predicate;
    }

    @Override
    public Duration visitTime_duration(WisdomQLParser.Time_durationContext ctx) {
        long value = Long.parseLong(ctx.INTEGER().getText());
        TemporalUnit unit;
        if (ctx.MICROSECOND() != null) {
            unit = ChronoUnit.MICROS;
        } else if (ctx.MILLISECOND() != null) {
            unit = ChronoUnit.MILLIS;
        } else if (ctx.SECOND() != null) {
            unit = ChronoUnit.SECONDS;
        } else if (ctx.MINUTE() != null) {
            unit = ChronoUnit.MINUTES;
        } else if (ctx.HOUR() != null) {
            unit = ChronoUnit.HOURS;
        } else if (ctx.DAY() != null) {
            unit = ChronoUnit.DAYS;
        } else if (ctx.MONTH() != null) {
            unit = ChronoUnit.MONTHS;
        } else if (ctx.YEAR() != null) {
            unit = ChronoUnit.YEARS;
        } else {
            throw new WisdomParserException(ctx, "invalid time unit");
        }
        return Duration.of(value, unit);
    }

    @Override
    public Comparable visitWisdom_primitive(WisdomQLParser.Wisdom_primitiveContext ctx) {
        Comparable value;
        if (ctx.STRING() != null) {
            value = Utility.toString(ctx.STRING().getText());
        } else if (ctx.NUMBER() != null) {
            value = Double.valueOf(ctx.NUMBER().getText());
        } else if (ctx.TRUE() != null) {
            value = true;
        } else if (ctx.FALSE() != null) {
            value = false;
        } else {
            throw new WisdomParserException(ctx, "invalid primitive data");
        }
        return value;
    }
}
