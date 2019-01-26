/*
 * Copyright (c) 2018, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
 *
 * Gobinath licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. In addition, if you are using
 * this file in your research work, you are required to cite
 * WISDOM as mentioned at https://github.com/slgobinath/wisdom.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.javahelps.wisdom.query.antlr;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Attribute;
import com.javahelps.wisdom.core.operand.WisdomArray;
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
import java.util.ArrayList;
import java.util.List;

import static com.javahelps.wisdom.core.util.WisdomConstants.*;
import static com.javahelps.wisdom.query.util.Constants.ANNOTATION.NAME;
import static com.javahelps.wisdom.query.util.Constants.ANNOTATION.VERSION;
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
            element.setValue(visit(ctx.wisdom_primitive()));
        } else if (ctx.variable_reference() != null) {
            element.setValue(visit(ctx.variable_reference()));
        } else if (ctx.array() != null) {
            element.setValue(visit(ctx.array()));
        }
        return element;
    }

    @Override
    public Statement visitSelect_statement(WisdomQLParser.Select_statementContext ctx) {
        SelectStatement selectStatement = new SelectStatement();
        if (ctx.attribute_name() != null && !ctx.attribute_name().isEmpty()) {
            for (WisdomQLParser.Attribute_nameContext attribute : ctx.attribute_name()) {
                selectStatement.addAttribute(attribute.getText());
            }
        } else if (ctx.real_int() != null && !ctx.real_int().isEmpty()) {
            for (WisdomQLParser.Real_intContext num : ctx.real_int()) {
                try {
                    int index = Integer.parseInt(num.getText());
                    selectStatement.addIndex(index);
                } catch (NumberFormatException ex) {
                    throw new WisdomParserException(ctx, "index must be an integer but found " + num.getText());
                }
            }
        }
        return selectStatement;
    }

    @Override
    public Statement visitFilter_statement(WisdomQLParser.Filter_statementContext ctx) {
        return new FilterStatement((LogicalOperator) visit(ctx.logical_operator()));
    }

    @Override
    public Statement visitLimit_statement(WisdomQLParser.Limit_statementContext ctx) {
        final int noOfBounds = ctx.INTEGER().size();
        int[] bounds = new int[noOfBounds];
        for (int i = 0; i < noOfBounds; i++) {
            bounds[i] = Integer.parseInt(ctx.INTEGER(i).getText());
        }
        return new LimitStatement(bounds);
    }

    @Override
    public Statement visitEnsure_statement(WisdomQLParser.Ensure_statementContext ctx) {
        final int noOfBounds = ctx.INTEGER().size();
        int[] bounds = new int[noOfBounds];
        for (int i = 0; i < noOfBounds; i++) {
            bounds[i] = Integer.parseInt(ctx.INTEGER(i).getText());
        }
        return new EnsureStatement(bounds);
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
        PartitionStatement statement;
        if (ctx.ADD() != null && ctx.ADD().size() > 0) {
            statement = new PartitionByValueStatement();
        } else {
            statement = new PartitionByAttributeStatement();
        }
        for (TerminalNode node : ctx.NAME()) {
            statement.addAttribute(node.getText());
        }
        return statement;
    }

    @Override
    public PatternOperator visitWisdom_pattern(WisdomQLParser.Wisdom_patternContext ctx) {
        if (ctx.name != null) {
            String streamName = ctx.name.getText();
            LogicalOperator logicalOperator = ctx.logical_operator() != null ? visitLogical_operator(ctx.logical_operator()) : null;
            String alias = ctx.NAME().size() == 2 ? ctx.NAME(1).getText() : streamName;

            PatternOperator operator = PatternOperator.create(streamName, logicalOperator, alias);

            if (ctx.INTEGER().size() == 2) {
                Long minCount = (Long) Utility.parseNumber(ctx.min.getText());
                Long maxCount = (Long) Utility.parseNumber(ctx.max.getText());
                operator.setMinCount(minCount);
                operator.setMaxCount(maxCount);

            } else if (ctx.INTEGER().size() == 1) {
                Long minCount = 0L;
                Long maxCount = Long.MAX_VALUE;
                if (ctx.min == null && ctx.max == null) {
                    minCount = (Long) Utility.parseNumber(ctx.INTEGER(0).getText());
                    maxCount = minCount;
                } else if (ctx.max == null) {
                    minCount = (Long) Utility.parseNumber(ctx.min.getText());
                } else {
                    maxCount = (Long) Utility.parseNumber(ctx.max.getText());
                }
                operator.setMinCount(minCount);
                operator.setMaxCount(maxCount);
            }

            if (ctx.NOT() != null) {
                operator = PatternOperator.not(operator, null);
            }
            return operator;

        } else if (ctx.EVERY() != null) {
            return PatternOperator.every(visitWisdom_pattern(ctx.wisdom_pattern().get(0)));
        } else if (ctx.NOT() != null) {
            Duration duration = ctx.time_duration() != null ? visitTime_duration(ctx.time_duration()) : null;
            return PatternOperator.not(visitWisdom_pattern(ctx.wisdom_pattern().get(0)), duration);
        } else if (ctx.AND() != null) {
            return PatternOperator.and(visitWisdom_pattern(ctx.wisdom_pattern().get(0)), visitWisdom_pattern(ctx.wisdom_pattern().get(1)));
        } else if (ctx.OR() != null) {
            return PatternOperator.or(visitWisdom_pattern(ctx.wisdom_pattern().get(0)), visitWisdom_pattern(ctx.wisdom_pattern().get(1)));
        } else if (ctx.ARROW() != null) {
            Duration duration = ctx.time_duration() != null ? visitTime_duration(ctx.time_duration()) : null;
            return PatternOperator.follows(visitWisdom_pattern(ctx.wisdom_pattern().get(0)), visitWisdom_pattern(ctx.wisdom_pattern().get(1)), duration);
        } else {
            return visitWisdom_pattern(ctx.wisdom_pattern(0));
        }
    }

    @Override
    public Statement visitAggregate_statement(WisdomQLParser.Aggregate_statementContext ctx) {
        AggregateStatement statement = new AggregateStatement();
        for (ParseTree tree : ctx.aggregate_operator()) {
            statement.addOperator((AggregateOperatorNode) visit(tree));
        }
        return statement;
    }

    @Override
    public Statement visitMap_statement(WisdomQLParser.Map_statementContext ctx) {
        MapStatement statement = new MapStatement();
        for (ParseTree tree : ctx.map_operator()) {
            statement.addOperator((MapOperator) visit(tree));
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
        } else if (ctx.map_statement() != null) {
            return (Statement) visit(ctx.map_statement());
        } else if (ctx.limit_statement() != null) {
            return (Statement) visit(ctx.limit_statement());
        } else if (ctx.ensure_statement() != null) {
            return (Statement) visit(ctx.ensure_statement());
        } else {
            throw new WisdomParserException(ctx, "unknown query statement");
        }
    }

    @Override
    public QueryNode visitQuery(WisdomQLParser.QueryContext ctx) {
        QueryNode queryNode;
        if (ctx.input != null) {
            // From stream
            queryNode = new QueryNode(ctx.input.getText());
        } else {
            // From definePattern
            queryNode = new QueryNode(visitWisdom_pattern(ctx.wisdom_pattern()));
        }
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

        if (ctx.operator != null) {
            operator = new LogicalOperator(ctx.operator.getText().toLowerCase());
        } else if (ctx.NOT() != null) {
            operator = new LogicalOperator("not");
        } else if (ctx.AND() != null) {
            operator = new LogicalOperator("and");
        } else if (ctx.OR() != null) {
            operator = new LogicalOperator("or");
        } else {
            operator = new LogicalOperator(null);
        }

        if (noOfLogicalOperators == 1) {
            operator.setLeft(visit(ctx.logical_operator(0)));
        } else if (noOfLogicalOperators == 2) {
            operator.setLeft(visit(ctx.logical_operator(0)));
            operator.setRight(visit(ctx.logical_operator(1)));
        } else if (noOfLogicalOperators == 0) {
            if (ctx.lft_name != null) {
                operator.setLeft(Attribute.of(ctx.lft_name.getText()));
            } else if (ctx.lft_primitive != null) {
                operator.setLeft(visit(ctx.lft_primitive));
            } else if (ctx.lft_string != null) {
                operator.setLeft(Utility.toString(ctx.lft_string.getText()));
            } else if (ctx.lft_var != null) {
                operator.setLeft(visit(ctx.lft_var));
            } else if (ctx.lft_array != null) {
                operator.setLeft(visit(ctx.lft_array));
            }
            if (ctx.rgt_name != null) {
                operator.setRight(Attribute.of(ctx.rgt_name.getText()));
            } else if (ctx.rgt_primitive != null) {
                operator.setRight(visit(ctx.rgt_primitive));
            } else if (ctx.rgt_string != null) {
                operator.setRight(Utility.toString(ctx.rgt_string.getText()));
            } else if (ctx.rgt_var != null) {
                operator.setRight(visit(ctx.rgt_var));
            } else if (ctx.rgt_array != null) {
                operator.setRight(visit(ctx.rgt_array));
            }
        }
        return operator;
    }

    @Override
    public MapOperator visitMap_operator(WisdomQLParser.Map_operatorContext ctx) {
        MapOperator operator;
        if (ctx.IF() != null) {
            operator = (MapOperator) visit(ctx.map_operator());
            operator.setLogicalOperator((LogicalOperator) visit(ctx.logical_operator()));
        } else {
            operator = new MapOperator();
            if (ctx.namespace == null) {
                if (ctx.attribute_name() != null) {
                    operator.setNamespace("rename");
                    operator.addProperty(KeyValueElement.of(ATTR, ctx.attribute_name().getText()));
                    operator.setAttrName(ctx.attr.getText());
                } else if (ctx.wisdom_primitive() != null) {
                    operator.setNamespace("constant");
                    operator.addProperty(KeyValueElement.of(VALUE, (Comparable) visit(ctx.wisdom_primitive())));
                    operator.setAttrName(ctx.attr.getText());
                } else if (ctx.variable_reference() != null) {
                    operator.setNamespace("variable");
                    operator.addProperty(KeyValueElement.of(VARIABLE, ((VariableReference) visit(ctx.variable_reference())).getVariableId()));
                    operator.setAttrName(ctx.attr.getText());
                }
            } else {
                operator.setNamespace(ctx.namespace.getText());
                operator.setAttrName(ctx.attr.getText());
                for (ParseTree tree : ctx.optional_key_value_element()) {
                    operator.addProperty((KeyValueElement) visit(tree));
                }
            }
        }
        return operator;
    }

    @Override
    public AggregateOperatorNode visitAggregate_operator(WisdomQLParser.Aggregate_operatorContext ctx) {

        AggregateOperatorNode node = new AggregateOperatorNode();
        node.setNamespace(ctx.namespace.getText());
        node.setAttrName(ctx.attr.getText());
        for (ParseTree tree : ctx.optional_key_value_element()) {
            node.addProperty((KeyValueElement) visit(tree));
        }
        return node;
    }

    @Override
    public Duration visitTime_duration(WisdomQLParser.Time_durationContext ctx) {
        long value;
        try {
            value = Long.parseLong(ctx.INTEGER().getText());
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
        return Duration.of(value, unit);
    }

    @Override
    public Object visitWisdom_operand(WisdomQLParser.Wisdom_operandContext ctx) {
        return super.visitWisdom_operand(ctx);
    }

    @Override
    public EventFunctionNode visitEvent_funtion(WisdomQLParser.Event_funtionContext ctx) {
        EventFunctionNode node = new EventFunctionNode();
        if (ctx.namespace == null) {
            node.setNamespace("constant");
            node.addProperty(KeyValueElement.of(VALUE, visit(ctx.wisdom_constant())));
        } else {
            node.setNamespace(ctx.namespace.getText());
            for (ParseTree tree : ctx.optional_key_value_element()) {
                node.addProperty((KeyValueElement) visit(tree));
            }
        }
        return node;
    }

    @Override
    public Object visitWisdom_constant(WisdomQLParser.Wisdom_constantContext ctx) {
        if (ctx.wisdom_primitive() != null) {
            return visit(ctx.wisdom_primitive());
        } else {
            return visit(ctx.array());
        }
    }

    @Override
    public Comparable visitWisdom_primitive(WisdomQLParser.Wisdom_primitiveContext ctx) {
        Comparable value;
        if (ctx.STRING() != null) {
            value = Utility.toString(ctx.STRING().getText());
        } else if (ctx.real_int() != null) {
            value = Utility.parseNumber(ctx.real_int().getText());
        } else if (ctx.real_float() != null) {
            value = Utility.parseNumber(ctx.real_float().getText());
        } else if (ctx.TRUE() != null) {
            value = true;
        } else if (ctx.FALSE() != null) {
            value = false;
        } else if (ctx.time_duration() != null) {
            value = ((Duration) visit(ctx.time_duration())).toMillis();
        } else if (ctx.NULL() != null) {
            return null;
        } else {
            throw new WisdomParserException(ctx, "invalid primitive data");
        }
        return value;
    }


    @Override
    public WisdomArray visitArray(WisdomQLParser.ArrayContext ctx) {
        int size = ctx.wisdom_primitive().size();
        List<Object> list = new ArrayList<>(size);
        for (ParseTree tree : ctx.wisdom_primitive()) {
            list.add(visit(tree));
        }
        return WisdomArray.of(list);
    }

    @Override
    public VariableReference visitVariable_reference(WisdomQLParser.Variable_referenceContext ctx) {
        return new VariableReference(ctx.NAME().getText());
    }
}
