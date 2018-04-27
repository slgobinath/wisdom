package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public class MapStatement implements Statement {

    private List<MapOperator> operators = new ArrayList<>();


    public void addOperator(MapOperator mapper) {
        this.operators.add(mapper);
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        int noOfOperators = operators.size();
        Mapper[] mappers = new Mapper[noOfOperators];
        for (int i = 0; i < noOfOperators; i++) {
            mappers[i] = operators.get(i).build(app, query);
        }
        query.map(mappers);
    }
}
