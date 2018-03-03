package com.javahelps.wisdom.query.util;

import com.javahelps.wisdom.query.antlr.WisdomParserException;
import com.javahelps.wisdom.query.tree.Annotation;
import org.antlr.v4.runtime.ParserRuleContext;

public class Utility {

    private Utility() {

    }

    public static void verifyAnnotation(ParserRuleContext ctx, Annotation annotation, String name, String... properties) {
        if (name.equals(annotation.getName())) {
            for (String key : properties) {
                if (!annotation.hasProperty(key)) {
                    throw new WisdomParserException(ctx, String.format("property bot found @%s in @app", key));
                }
            }
        } else {
            throw new WisdomParserException(ctx, String.format("required @%s, but found @%s", name, annotation.getName()));
        }
    }
}
