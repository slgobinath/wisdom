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
    public Object visitDefinition(WisdomQLParser.DefinitionContext ctx) {
        if (ctx.def_stream() != null) {
            return visit(ctx.def_stream());
        } else if (ctx.def_variable() != null) {
            return visit(ctx.def_variable());
        } else {
            throw new WisdomParserException(ctx, "unknown definition");
        }
    }

    @Override
    public VariableDefinition visitDef_variable(WisdomQLParser.Def_variableContext ctx) {
        VariableDefinition definition = new VariableDefinition(ctx.NAME().getText());
        if (ctx.STRING() != null) {
            definition.setValue(Utility.toString(ctx.STRING().getText()));
        } else if (ctx.NUMBER() != null) {
            definition.setValue(Double.parseDouble(ctx.NUMBER().getText()));
        } else {
            throw new WisdomParserException(ctx, "undefined variable");
        }
        return definition;
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
        if (ctx.STRING() != null) {
            element.setValue(Utility.toString(ctx.STRING().getText()));
        } else if (ctx.NUMBER() != null) {
            element.setValue(Double.parseDouble(ctx.NUMBER().getText()));
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
        return new FilterStatement((Predicate<Event>) visit(ctx.logical_operator()));
    }

    @Override
    public Statement visitQuery_statement(WisdomQLParser.Query_statementContext ctx) {
        if (ctx.select_statement() != null) {
            return (Statement) visit(ctx.select_statement());
        } else if (ctx.filter_statement() != null) {
            return (Statement) visit(ctx.filter_statement());
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
                    predicate = Operator.EQUAL_ATTRIBUTES(ctx.left.getText(), ctx.right.getText());
                } else {
                    if (ctx.NUMBER() != null) {
                        if (ctx.right.getType() == ctx.NUMBER().getSymbol().getType()) {
                            predicate = Operator.EQUALS(ctx.left.getText(), Double.parseDouble(ctx.right.getText()));
                        } else if (ctx.left.getType() == ctx.NUMBER().getSymbol().getType()) {
                            predicate = Operator.EQUALS(ctx.right.getText(), Double.parseDouble(ctx.left.getText()));
                        }
                    } else if (ctx.STRING() != null) {
                        if (ctx.right.getType() == ctx.STRING().getSymbol().getType()) {
                            predicate = Operator.EQUALS(ctx.left.getText(), Utility.toString(ctx.right.getText()));
                        } else if (ctx.left.getType() == ctx.STRING().getSymbol().getType()) {
                            predicate = Operator.EQUALS(ctx.right.getText(), Utility.toString(ctx.left.getText()));
                        }
                    } else if (ctx.TRUE() != null) {
                        predicate = Operator.IS_TRUE(ctx.NAME(0).getText());
                    } else if (ctx.FALSE() != null) {
                        predicate = Operator.IS_FALSE(ctx.NAME(0).getText());
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
}
