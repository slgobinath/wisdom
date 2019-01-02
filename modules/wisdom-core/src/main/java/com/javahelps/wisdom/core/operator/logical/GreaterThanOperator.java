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

package com.javahelps.wisdom.core.operator.logical;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.util.Commons;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

@WisdomExtension(">")
public class GreaterThanOperator extends LogicalOperator {

    private Predicate<Event> predicate;

    public GreaterThanOperator(Map<String, ?> properties) {
        super(properties);
        Object left = Commons.getProperty(properties, "left", 0);
        Object right = Commons.getProperty(properties, "right", 1);

        if (left instanceof Comparable) {
            // constant > right
            if (right instanceof Comparable) {
                boolean result = test(left, right);
                predicate = event -> result;
            } else if (right instanceof Function) {
                // function(attribute) in String
                predicate = event -> test(left, ((Function) right).apply(event));
            } else if (right instanceof Supplier) {
                // supplier(variable) in String
                predicate = event -> test(left, ((Supplier) right).get());
            } else {
                throw new WisdomAppValidationException("java.lang.Comparable > %s is not supported", right.getClass().getCanonicalName());
            }
        } else if (left instanceof Function) {
            // function(attribute) > right
            if (right instanceof Comparable) {
                // function(attribute) > constant
                predicate = event -> test(((Function) left).apply(event), right);
            } else if (right instanceof Function) {
                // function(attribute) > function(attribute)
                predicate = event -> test(((Function) left).apply(event), ((Function) right).apply(event));
            } else if (right instanceof Supplier) {
                // function(attribute) > supplier(variable)
                predicate = event -> test(((Function) left).apply(event), ((Supplier) right).get());
            } else {
                throw new WisdomAppValidationException("java.util.function.Function > %s is not supported", right.getClass().getCanonicalName());
            }
        } else if (left instanceof Supplier) {
            // supplier(variable) > right
            if (right instanceof Comparable) {
                // supplier(variable) > constant
                predicate = event -> test(((Supplier) left).get(), right);
            } else if (right instanceof Function) {
                // supplier(variable) > function(attribute)
                predicate = event -> test(((Supplier) left).get(), ((Function) right).apply(event));
            } else if (right instanceof Supplier) {
                // supplier(variable) > supplier(variable)
                predicate = event -> test(((Supplier) left).get(), ((Supplier) right).get());
            } else {
                throw new WisdomAppValidationException("java.util.function.Supplier > %s is not supported", right.getClass().getCanonicalName());
            }
        } else {
            throw new WisdomAppValidationException("%s > is not supported", right.getClass().getCanonicalName());
        }
    }

    private static boolean test(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return ((Number) left).doubleValue() > ((Number) right).doubleValue();
        } else if (left instanceof Comparable && right instanceof Comparable) {
            return ((Comparable) left).compareTo(right) > 0;
        } else {
            throw new WisdomAppRuntimeException("Cannot compare %s with $s", left.getClass().getCanonicalName(), right.getClass().getCanonicalName());
        }
    }

    @Override
    public boolean test(Event event) {
        return this.predicate.test(event);
    }
}
