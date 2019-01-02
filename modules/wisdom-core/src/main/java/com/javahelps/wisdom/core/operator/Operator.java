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

package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.operator.logical.*;
import com.javahelps.wisdom.core.processor.AttributeSelectProcessor;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static com.javahelps.wisdom.core.util.Commons.map;
import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

/**
 * {@link Operator} provides some built-in operations on the given attribute of an {@link Event} at the
 * runtime.
 * {@link Operator} modifies the attributes of the {@link Event} which is passed attrName the parameter of the
 * {@link Function}.
 *
 * @see AttributeSelectProcessor
 */
public class Operator {

    public static AggregateOperator SUM(final String attribute, final String as) {
        return new SumOperator(as, Map.of(ATTR, attribute));
    }

    public static AggregateOperator AVG(final String attribute, final String as) {
        return new AvgOperator(as, Map.of(ATTR, attribute));
    }

    public static AggregateOperator MIN(final String attribute, final String as) {
        return new MinOperator(as, Map.of(ATTR, attribute));
    }

    public static AggregateOperator MAX(final String attribute, final String as) {
        return new MaxOperator(as, Map.of(ATTR, attribute));
    }

    public static AggregateOperator COUNT(String as) {
        return new CountOperator(as, Collections.EMPTY_MAP);
    }

    public static AggregateOperator COLLECT(final String attribute, final String as) {
        return new CollectOperator(as, Map.of(ATTR, attribute));
    }

    public static LogicalOperator IN(Object left, Object right) {
        return new InOperator(Map.of("left", left, "right", right));
    }

    public static LogicalOperator IN(Object left) {
        return new InOperator(Map.of("left", left));
    }

    public static LogicalOperator MATCHES(Object regex, Object right) {
        return new RegexInOperator(Map.of("left", regex, "right", right));
    }

    public static LogicalOperator EQUALS(Object left, Object right) {
        return new EqualsOperator(map("left", left, "right", right));
    }

    public static LogicalOperator GREATER_THAN(Object left, Object right) {
        return new GreaterThanOperator(Map.of("left", left, "right", right));
    }

    public static LogicalOperator GREATER_THAN_OR_EQUAL(Object left, Object right) {
        return new GreaterThanOrEqualOperator(Map.of("left", left, "right", right));
    }

    public static LogicalOperator LESS_THAN(Object left, Object right) {
        return new LessThanOperator(Map.of("left", left, "right", right));
    }

    public static LogicalOperator LESS_THAN_OR_EQUAL(Object left, Object right) {
        return new LessThanOrEqualOperator(Map.of("left", left, "right", right));
    }
}
