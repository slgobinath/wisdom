package com.javahelps.wisdom.query.antlr;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.AggregateOperator;
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

import static com.javahelps.wisdom.query.util.Constants.ANNOTATION.*;

public class WisdomQLBaseVisitorImpl extends WisdomQLBaseVisitor {

    @Override
    public WisdomApp visitWisdom_app(WisdomQLParser.Wisdom_appContext ctx) {
        WisdomApp wisdomApp;
        // Create WisdomApp
        if (ctx.annotation() != null) {
            Annotation annotation = (Annotation) visit(ctx.annotation());
            Utility.verifyAnnotation(ctx.annotation(), annotation, APP_ANNOTATION, NAME, VERSION);
            wisdomApp = new WisdomApp(annotation.getProperties());
        } else {
            wisdomApp = new WisdomApp();
        }
        // Define components
        for (ParseTree tree : ctx.definition()) {
            Definition definition = (Definition) visit(tree);
            definition.define(wisdomApp);
        }
        // Create queries
        for (ParseTree tree : ctx.query()) {
            QueryNode queryNode = (QueryNode) visit(tree);
            Query query = wisdomApp.defineQuery(queryNode.getName());
            queryNode.build(wisdomApp, query);
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
        VariableDefinition definition = new VariableDefinition(ctx.NAME().getText(), (Comparable) visit(ctx.wisdom_primitive()));
        for (ParseTree tree : ctx.annotation()) {
            Annotation annotation = (Annotation) visit(tree);
            definition.addAnnotation(ctx, annotation);
        }
        return definition;
    }

    @Override
    public StreamDefinition visitDef_stream(WisdomQLParser.Def_streamContext ctx) {
        StreamDefinition definition = new StreamDefinition(ctx.NAME().getText());
        for (ParseTree tree : ctx.annotation()) {
            Annotation annotation = (Annotation) visit(tree);
            definition.addAnnotation(ctx, annotation);
        }
        return definition;
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
            element.setValue(visit(ctx.wisdom_primitive()));
        } else if (ctx.array() != null) {
            element.setValue(visit(ctx.array()));
        }
        return element;
    }

    @Override
    public KeyValueElement visitOptional_key_value_element(WisdomQLParser.Optional_key_value_elementContext ctx) {
        KeyValueElement element = new KeyValueElement();
        if (ctx.NAME() != null) {
            element.setKey(ctx.NAME().getText());
        }
        if (ctx.wisdom_primitive() != null) {
            element.setValue((Comparable) visit(ctx.wisdom_primitive()));
        } else if (ctx.variable_reference() != null) {
            element.setValue((Comparable) visit(ctx.variable_reference()));
            element.setVariable(true);
        }
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
        return new FilterStatement((LogicalOperator) visit(ctx.logical_operator()));
    }

    @Override
    public Statement visitWindow_statement(WisdomQLParser.Window_statementContext ctx) {
        WindowStatement statement = new WindowStatement(ctx.name.getText());
        if (ctx.type != null) {
            statement.setType(ctx.type.getText());
        }
        for (ParseTree tree : ctx.optional_key_value_element()) {
            statement.addProperty((KeyValueElement) visit(tree));
        }
        return statement;
    }

    @Override
    public Statement visitInsert_into_statement(WisdomQLParser.Insert_into_statementContext ctx) {
        return new InsertIntoStatement(ctx.NAME().getText());
    }

    @Override
    public Statement visitUpdate_statement(WisdomQLParser.Update_statementContext ctx) {
        return new UpdateStatement(ctx.NAME().getText());
    }

    @Override
    public Statement visitPartition_statement(WisdomQLParser.Partition_statementContext ctx) {
        PartitionStatement statement = new PartitionStatement();
        for (TerminalNode node : ctx.NAME()) {
            statement.addAttribute(node.getText());
        }
        return statement;
    }

    @Override
    public Statement visitAggregate_statement(WisdomQLParser.Aggregate_statementContext ctx) {
        AggregateStatement statement = new AggregateStatement();
        for (ParseTree tree : ctx.aggregate_operator()) {
            statement.addOperator((AggregateOperator) visit(tree));
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
        } else if (ctx.aggregate_statement() != null) {
            return (Statement) visit(ctx.aggregate_statement());
        } else if (ctx.partition_statement() != null) {
            return (Statement) visit(ctx.partition_statement());
        } else {
            throw new WisdomParserException(ctx, "unknown query statement");
        }
    }

    @Override
    public QueryNode visitQuery(WisdomQLParser.QueryContext ctx) {
        QueryNode queryNode = new QueryNode(ctx.input.getText());
        if (ctx.annotation() != null) {
            Annotation annotation = (Annotation) visit(ctx.annotation());
            Utility.verifyAnnotation(ctx.annotation(), annotation, QUERY_ANNOTATION, NAME);
            queryNode.setName(annotation.getProperty(NAME));
        }
        for (ParseTree tree : ctx.query_statement()) {
            queryNode.addStatement((Statement) visit(tree));
        }
        if (ctx.insert_into_statement() != null) {
            queryNode.addStatement((Statement) visit(ctx.insert_into_statement()));
        } else if (ctx.update_statement() != null) {
            queryNode.addStatement((Statement) visit(ctx.update_statement()));
        }
        return queryNode;
    }

    @Override
    public LogicalOperator visitLogical_operator(WisdomQLParser.Logical_operatorContext ctx) {
        LogicalOperator operator;
        int noOfLogicalOperators = ctx.logical_operator().size();

        if (ctx.NOT() != null) {
            operator = new LogicalOperator(LogicalOperator.Operation.NOT);
        } else if (ctx.AND() != null) {
            operator = new LogicalOperator(LogicalOperator.Operation.AND);
        } else if (ctx.OR() != null) {
            operator = new LogicalOperator(LogicalOperator.Operation.OR);
        } else if (ctx.GREATER_THAN() != null) {
            operator = new LogicalOperator(LogicalOperator.Operation.GT);
        } else if (ctx.LESS_THAN() != null) {
            operator = new LogicalOperator(LogicalOperator.Operation.LT);
        } else if (ctx.GT_EQ() != null) {
            operator = new LogicalOperator(LogicalOperator.Operation.GT_EQ);
        } else if (ctx.LT_EQ() != null) {
            operator = new LogicalOperator(LogicalOperator.Operation.LT_EQ);
        } else if (ctx.EQUALS() != null) {
            operator = new LogicalOperator(LogicalOperator.Operation.EQ);
        } else if (ctx.IN() != null) {
            operator = new LogicalOperator(LogicalOperator.Operation.IN);
        } else if (noOfLogicalOperators == 1) {
            operator = new LogicalOperator(LogicalOperator.Operation.IDENTICAL);
        } else {
            throw new WisdomParserException(ctx, "unknown logical operator");
        }


        if (noOfLogicalOperators == 1) {
            operator.setLeftOperator((LogicalOperator) visit(ctx.logical_operator(0)));
        } else if (noOfLogicalOperators == 2) {
            operator.setLeftOperator((LogicalOperator) visit(ctx.logical_operator(0)));
            operator.setRightOperator((LogicalOperator) visit(ctx.logical_operator(1)));
        } else if (noOfLogicalOperators == 0) {
            if (ctx.lft_name != null) {
                operator.setLeftAttr(ctx.lft_name.getText());
            } else if (ctx.lft_number != null) {
                operator.setLeftComparable(Double.parseDouble(ctx.lft_number.getText()));
            } else if (ctx.lft_string != null) {
                operator.setLeftComparable(Utility.toString(ctx.lft_string.getText()));
            } else if (ctx.lft_pri != null) {
                operator.setLeftComparable((Comparable) visit(ctx.lft_pri));
            } else if (ctx.lft_var != null) {
                operator.setLeftVar((String) visit(ctx.lft_var));
            }
            if (ctx.rgt_name != null) {
                operator.setRightAttr(ctx.rgt_name.getText());
            } else if (ctx.rgt_number != null) {
                operator.setRightComparable(Double.parseDouble(ctx.rgt_number.getText()));
            } else if (ctx.rgt_string != null) {
                operator.setRightComparable(Utility.toString(ctx.rgt_string.getText()));
            } else if (ctx.rgt_pri != null) {
                operator.setRightComparable((Comparable) visit(ctx.rgt_pri));
            } else if (ctx.rgt_var != null) {
                operator.setRightVar((String) visit(ctx.rgt_var));
            }
        }
        return operator;
    }

    @Override
    public AggregateOperator visitSum_operator(WisdomQLParser.Sum_operatorContext ctx) {
        return Operator.SUM(ctx.NAME(0).getText(), ctx.NAME(1).getText());
    }

    @Override
    public AggregateOperator visitAvg_operator(WisdomQLParser.Avg_operatorContext ctx) {
        return Operator.AVG(ctx.NAME(0).getText(), ctx.NAME(1).getText());
    }

    @Override
    public AggregateOperator visitMax_operator(WisdomQLParser.Max_operatorContext ctx) {
        return Operator.MAX(ctx.NAME(0).getText(), ctx.NAME(1).getText());
    }

    @Override
    public AggregateOperator visitMin_operator(WisdomQLParser.Min_operatorContext ctx) {
        return Operator.MIN(ctx.NAME(0).getText(), ctx.NAME(1).getText());
    }

    @Override
    public AggregateOperator visitCount_operator(WisdomQLParser.Count_operatorContext ctx) {
        return Operator.COUNT(ctx.NAME().getText());
    }

    @Override
    public AggregateOperator visitAggregate_operator(WisdomQLParser.Aggregate_operatorContext ctx) {
        if (ctx.avg_operator() != null) {
            return (AggregateOperator) visit(ctx.avg_operator());
        } else if (ctx.sum_operator() != null) {
            return (AggregateOperator) visit(ctx.sum_operator());
        } else if (ctx.max_operator() != null) {
            return (AggregateOperator) visit(ctx.max_operator());
        } else if (ctx.min_operator() != null) {
            return (AggregateOperator) visit(ctx.min_operator());
        } else if (ctx.count_operator() != null) {
            return (AggregateOperator) visit(ctx.count_operator());
        } else {
            throw new WisdomParserException(ctx, "unknown aggregate operation");
        }
    }

    @Override
    public Long visitTime_duration(WisdomQLParser.Time_durationContext ctx) {
        long value;
        try {
            value = Long.parseLong(ctx.NUMBER().getText());
        } catch (NumberFormatException ex) {
            throw new WisdomParserException(ctx, "time duration must be integer");
        }
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
        return Duration.of(value, unit).toMillis();
    }

    @Override
    public Comparable visitWisdom_primitive(WisdomQLParser.Wisdom_primitiveContext ctx) {
        Comparable value;
        if (ctx.STRING() != null) {
            value = Utility.toString(ctx.STRING().getText());
        } else if (ctx.NUMBER() != null) {
            try {
                value = Long.parseLong(ctx.NUMBER().getText());
            } catch (NumberFormatException ex) {
                value = Double.valueOf(ctx.NUMBER().getText());
            }
        } else if (ctx.TRUE() != null) {
            value = true;
        } else if (ctx.FALSE() != null) {
            value = false;
        } else if (ctx.time_duration() != null) {
            value = (Comparable) visit(ctx.time_duration());
        } else {
            throw new WisdomParserException(ctx, "invalid primitive data");
        }
        return value;
    }

    @Override
    public Comparable[] visitArray(WisdomQLParser.ArrayContext ctx) {
        int size = ctx.wisdom_primitive().size();
        Comparable[] array = new Comparable[size];
        for (int i = 0; i < size; i++) {
            array[i] = (Comparable) visit(ctx.wisdom_primitive(i));
        }
        return array;
    }

    @Override
    public Object visitVariable_reference(WisdomQLParser.Variable_referenceContext ctx) {
        return ctx.NAME().getText();
    }
}
