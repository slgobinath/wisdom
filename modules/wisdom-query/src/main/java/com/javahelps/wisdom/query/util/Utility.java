package com.javahelps.wisdom.query.util;

import com.javahelps.wisdom.query.antlr.WisdomParserException;
import com.javahelps.wisdom.query.tree.Annotation;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Utility {

    private Utility() {

    }

    public static void verifyAnnotation(ParserRuleContext ctx, Annotation annotation, String name, String... properties) {
        if (name.equals(annotation.getName())) {
            for (String key : properties) {
                if (!annotation.hasProperty(key)) {
                    throw new WisdomParserException(ctx, String.format("property not found @%s in @%s", key,
                            annotation.getName()));
                }
            }
        } else {
            throw new WisdomParserException(ctx, String.format("required @%s, but found @%s", name, annotation.getName()));
        }
    }

    public static String toString(String str) {
        if (str.startsWith("\"")) {
            str = str.replaceAll("^\"|\"$", "");
        } else if (str.startsWith("'")) {
            str = str.replaceAll("^'|'$", "");
        }
        return str;
    }

    public static Map<String, Comparable> toMap(Properties properties) {
        Map<String, Comparable> map = new HashMap<String, Comparable>();
        for (String key : properties.stringPropertyNames()) {
            map.put(key, (Comparable) properties.get(key));
        }
        return map;
    }
}