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
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.query.Query;

import java.util.function.Predicate;

import static com.javahelps.wisdom.core.util.Commons.map;

public class LogicalOperator implements OperatorElement {


    private final String operator;
    private Object left;
    private Object right;

    public LogicalOperator(String operator) {
        this.operator = operator;
    }

    public void setLeft(Object left) {
        this.left = left;
    }

    public void setRight(Object right) {
        this.right = right;
    }


    public Predicate<Event> build(WisdomApp app, Query query) {
        if (this.operator == null) {
            return ((LogicalOperator) this.left).build(app, query);
        } else if ("not".equals(this.operator)) {
            return ((LogicalOperator) this.left).build(app, query).negate();
        } else if ("and".equals(this.operator)) {
            return ((LogicalOperator) this.left).build(app, query).and(((LogicalOperator) this.right).build(app, query));
        } else if ("or".equals(this.operator)) {
            return ((LogicalOperator) this.left).build(app, query).or(((LogicalOperator) this.right).build(app, query));
        } else {
            if (left instanceof VariableReference) {
                left = ((VariableReference) left).build(app);
            }
            if (right instanceof VariableReference) {
                right = ((VariableReference) right).build(app);
            }
            return com.javahelps.wisdom.core.operator.logical.LogicalOperator.create(this.operator, map("left", left, "right", right));
        }
    }
}
