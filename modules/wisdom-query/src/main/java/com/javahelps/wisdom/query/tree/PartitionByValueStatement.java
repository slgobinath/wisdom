package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.query.Query;

public class PartitionByValueStatement extends PartitionStatement {

    @Override
    public void addTo(WisdomApp app, Query query) {
        query.partitionByVal(this.attributes.toArray(new String[0]));
    }
}
