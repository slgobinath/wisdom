package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.query.Query;

import java.util.function.Predicate;

public class FilterStatement implements Statement {

    private final Predicate<Event> predicate;

    public FilterStatement(Predicate<Event> predicate) {
        this.predicate = predicate;
    }

    @Override
    public void addTo(Query query) {
        query.filter(this.predicate);
    }
}
