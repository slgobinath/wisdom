package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.query.antlr.WisdomParserException;
import org.antlr.v4.runtime.ParserRuleContext;

public class VariableDefinition extends Definition {

    private final Comparable value;

    public VariableDefinition(String name, Comparable value) {
        super(name);
        this.value = value;
    }

    public Comparable getValue() {
        return value;
    }

    @Override
    protected void verifyAnnotation(ParserRuleContext ctx, Annotation annotation) {
        throw new WisdomParserException(ctx, String.format("@%s is not supported for Variable", annotation.getName()));
    }

    @Override
    public void define(WisdomApp app) {
        if (this.configuration == null) {
            app.defineVariable(this.getName(), this.value);
        } else {
            app.defineVariable(this.getName(), this.value, this.configuration);
        }
    }
}
