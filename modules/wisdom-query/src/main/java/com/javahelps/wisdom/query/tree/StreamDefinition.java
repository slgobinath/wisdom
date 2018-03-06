package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;

public class StreamDefinition extends Definition {

    private Annotation annotation;

    public StreamDefinition(String name) {
        super(name);
    }

    public void setAnnotation(Annotation annotation) {
        this.annotation = annotation;
    }

    @Override
    public void define(WisdomApp app) {
        if (this.annotation == null) {
            app.defineStream(this.getName());
        } else {
            app.defineStream(this.getName(), this.annotation.getProperties());
        }
    }
}
