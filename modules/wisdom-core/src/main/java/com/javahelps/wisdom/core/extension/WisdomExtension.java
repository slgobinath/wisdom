package com.javahelps.wisdom.core.extension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface WisdomExtension {

    /**
     * Namespace of the extension.
     *
     * @return the namespace
     */
    String value();
}
