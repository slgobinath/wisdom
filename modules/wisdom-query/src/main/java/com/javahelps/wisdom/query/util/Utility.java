package com.javahelps.wisdom.query.util;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.query.antlr.WisdomParserException;
import com.javahelps.wisdom.query.tree.Annotation;
import com.javahelps.wisdom.query.tree.KeyValueElement;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.text.StringEscapeUtils;

import java.util.HashMap;
import java.util.List;
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

    /**
     * Parse Wisdom query String constants and unescape special characters.
     *
     * @param str String constant
     * @return unescaped string
     */
    public static String toString(String str) {
        if (str.startsWith("\"\"\"")) {
            str = str.replaceAll("^\"\"\"|\"\"\"$", "");
        } else if (str.startsWith("\"")) {
            str = str.replaceAll("^\"|\"$", "");
        } else if (str.startsWith("'")) {
            str = str.replaceAll("^'|'$", "");
        } else if (str.startsWith("'''")) {
            str = str.replaceAll("^'''|'''$", "");
        }
        str = StringEscapeUtils.unescapeJava(str);
        return str;
    }

    public static Map<String, Comparable> toMap(Properties properties) {
        Map<String, Comparable> map = new HashMap<String, Comparable>();
        for (String key : properties.stringPropertyNames()) {
            map.put(key, (Comparable) properties.get(key));
        }
        return map;
    }

    public static Map<String, Object> toProperties(WisdomApp app, List<KeyValueElement> keyValueElements) {
        int count = 0;
        Map<String, Object> properties = new HashMap<>(keyValueElements.size());
        for (KeyValueElement element : keyValueElements) {
            String key = element.getKey();
            if (key == null) {
                key = String.format("_param_%d", count);
            }
            if (element.isVariable()) {
                properties.put(key, app.getVariable((String) element.getValue()));
            } else {
                properties.put(key, element.getValue());
            }
            count++;
        }
        return properties;
    }
}
