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
import com.javahelps.wisdom.core.event.RuntimeAttribute;
import com.javahelps.wisdom.core.query.Query;

import java.util.function.Predicate;

import static com.javahelps.wisdom.core.util.Commons.map;

public class LogicalOperator implements OperatorElement {


    private final String operator;
    private Object left;
    private Object right;
    private boolean readFromSupplier = false;

    public LogicalOperator(String operator) {
        this.operator = operator;
    }

    public void setLeft(Object left) {
        this.left = left;
    }

    public void setRight(Object right) {
        this.right = right;
    }

    public void setReadFromSupplier(boolean readFromSupplier) {
        this.readFromSupplier = readFromSupplier;
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
            left = convertIfReference(left, app, query);
            right = convertIfReference(right, app, query);
            return com.javahelps.wisdom.core.operator.logical.LogicalOperator.create(this.operator, map("left", left, "right", right));
        }
    }

    private Object convertIfReference(Object operand, WisdomApp app, Query query) {
        if (operand instanceof VariableReference) {
            operand = ((VariableReference) operand).build(app);
        } else if (operand instanceof RuntimeAttribute && this.readFromSupplier) {
            String name = ((RuntimeAttribute) operand).getName();
            int splitIndex = name.indexOf('.');
            if (splitIndex >= 0) {
                String supplierId = name.substring(0, splitIndex);
                operand = query.getAttributeSupplier(supplierId).of(name);
            }
        }
        return operand;
    }
}
