package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.query.antlr.WisdomParserException;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.javahelps.wisdom.query.util.Constants.ANNOTATION.CONFIG_ANNOTATION;

public abstract class Definition {

    private final String name;
    protected Properties configuration;
    protected List<Annotation> annotations = new ArrayList<>();

    protected Definition(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public abstract void define(WisdomApp app);

    public final void addAnnotation(ParserRuleContext ctx, Annotation annotation) {
        if (CONFIG_ANNOTATION.equals(annotation.getName())) {
            if (this.configuration != null) {
                throw new WisdomParserException(ctx, "@" + CONFIG_ANNOTATION + " is already defined for " + getName());
            }
            this.configuration = annotation.getProperties();
        } else {
            this.verifyAnnotation(ctx, annotation);
            this.annotations.add(annotation);
        }
    }

    protected abstract void verifyAnnotation(ParserRuleContext ctx, Annotation annotation);
}
