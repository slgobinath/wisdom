/*
 * Copyright (c) 2018, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
 *
 * Gobinath licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. In addition, if you are using
 * this file in your research work, you are required to cite
 * WISDOM as mentioned at https://github.com/slgobinath/wisdom.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
