package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;

import java.util.function.Predicate;

/**
 * CustomPattern.
 */
class CustomPattern extends Pattern {

    CustomPattern(String patternId) {
        super(patternId);
    }

    @Override
    public Pattern filter(Predicate<Event> predicate) {

        throw new WisdomAppValidationException(this.getClass().getSimpleName() + " cannot have filter");
    }
}
