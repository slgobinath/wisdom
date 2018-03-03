package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.query.Query;

public interface Statement {
    public void addTo(Query query);
}
