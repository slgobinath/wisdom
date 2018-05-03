package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.query.util.Utility;

import java.util.ArrayList;
import java.util.List;

public class MapOperator implements OperatorElement {

    private String namespace;
    private String attrName;
    private List<KeyValueElement> keyValueElements = new ArrayList<>();
    private LogicalOperator logicalOperator;

    public void addProperty(KeyValueElement element) {
        this.keyValueElements.add(element);
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public void setLogicalOperator(LogicalOperator logicalOperator) {
        this.logicalOperator = logicalOperator;
    }

    public Mapper build(WisdomApp app, Query query) {
        Mapper mapper = Mapper.create(namespace, attrName, Utility.toProperties(app, keyValueElements));
        if (logicalOperator != null) {
            mapper.onlyIf(logicalOperator.build(app, query));
        }
        return mapper;
    }
}
