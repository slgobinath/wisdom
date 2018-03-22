package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.query.antlr.WisdomParserException;
import com.javahelps.wisdom.query.util.Utility;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.javahelps.wisdom.query.util.Constants.ANNOTATION.*;

public class StreamDefinition extends Definition {

    private Annotation annotation;
    private Properties configuration;
    private List<Annotation> annotations = new ArrayList<>();

    public StreamDefinition(String name) {
        super(name);
    }

    public void addAnnotation(ParserRuleContext ctx, Annotation annotation) {
        if (CONFIG_ANNOTATION.equals(annotation.getName())) {
            if (this.configuration != null) {
                throw new WisdomParserException(ctx, "@" + CONFIG_ANNOTATION + " is already defined for " + getName());
            }
            this.configuration = annotation.getProperties();
        } else {
            if (SINK_ANNOTATION.equals(annotation.getName())) {
                Utility.verifyAnnotation(ctx, annotation, SINK_ANNOTATION, TYPE);
            }
            this.annotations.add(annotation);
        }
    }

    @Override
    public void define(WisdomApp app) {
        if (this.configuration == null) {
            app.defineStream(this.getName());
        } else {
            app.defineStream(this.getName(), this.configuration);
        }
        for (Annotation annotation : this.annotations) {
            if (SINK_ANNOTATION.equals(annotation.getName())) {
                app.addSink(getName(), Sink.create(annotation.getProperty(TYPE), Utility.toMap(annotation.getProperties())));
            }
        }
    }
}
