package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.AggregateOperator;
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.query.util.Utility;

import java.util.ArrayList;
import java.util.List;

public class AggregateOperatorNode implements OperatorElement {

    private String namespace;
    private String attrName;
    private List<KeyValueElement> keyValueElements = new ArrayList<>();

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

    public AggregateOperator build(WisdomApp app, Query query) {
        return AggregateOperator.create(namespace, attrName, Utility.toProperties(app, keyValueElements));
    }
}
