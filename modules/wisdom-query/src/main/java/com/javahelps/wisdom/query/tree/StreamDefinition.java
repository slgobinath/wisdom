package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.query.util.Utility;
import org.antlr.v4.runtime.ParserRuleContext;

import static com.javahelps.wisdom.query.util.Constants.ANNOTATION.*;

public class StreamDefinition extends Definition {

    public StreamDefinition(String name) {
        super(name);
    }

    @Override
    protected void verifyAnnotation(ParserRuleContext ctx, Annotation annotation) {
        if (SINK_ANNOTATION.equals(annotation.getName())) {
            Utility.verifyAnnotation(ctx, annotation, SINK_ANNOTATION, TYPE);
        } else if (SOURCE_ANNOTATION.equals(annotation.getName())) {
            Utility.verifyAnnotation(ctx, annotation, SOURCE_ANNOTATION, TYPE);
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
            } else if (SOURCE_ANNOTATION.equals(annotation.getName())) {
                app.addSource(getName(), Source.create(annotation.getProperty(TYPE), Utility.toMap(annotation.getProperties())));
            }
        }
    }
}
