package com.javahelps.wisdom.core.util;

import java.util.function.Consumer;
import java.util.function.Predicate;

public class FunctionalUtility {

    private FunctionalUtility() {

    }

    public static <E> Consumer<E> silentConsumer() {
        return e -> {
        };
    }

    public static <E> Predicate<E> truePredicator() {
        return e -> true;
    }
}
