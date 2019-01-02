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
import java.util.regex.Pattern;

@WisdomExtension("matches")
public class RegexInOperator extends LogicalOperator {

    private Predicate<Event> predicate;

    public RegexInOperator(Map<String, ?> properties) {
        super(properties);
        Object left = Commons.getProperty(properties, "left", 0);
        Object right = Commons.getProperty(properties, "right", 1);

        if (left instanceof String) {
            // constant matches right
            Pattern pattern = Pattern.compile((String) left);
            if (right instanceof String) {
                boolean result = pattern.matcher((CharSequence) right).find();
                predicate = event -> result;
            } else if (right instanceof Function) {
                // constant matches function(attribute)
                predicate = event -> {
                    Object value = ((Function) right).apply(event);
                    if (value instanceof String) {
                        return pattern.matcher((CharSequence) value).find();
                    } else {
                        return false;
                    }
                };
            } else if (right instanceof Supplier) {
                // constant matches supplier(variable)
                predicate = event -> {
                    Object value = ((Supplier) right).get();
                    if (value instanceof String) {
                        return pattern.matcher((CharSequence) value).find();
                    } else {
                        return false;
                    }
                };
            } else {
                throw new WisdomAppValidationException("java.lang.String MATCHES %s is not supported", right.getClass().getCanonicalName());
            }
        } else if (left instanceof Function) {
            // function(attribute) matches right
            if (right instanceof String) {
                // function(attribute) matches constant
                predicate = event -> {
                    Object regex = ((Function) left).apply(event);
                    return test(regex, right);
                };
            } else if (right instanceof Function) {
                // function(attribute) matches function(attribute)
                predicate = event -> {
                    Object regex = ((Function) left).apply(event);
                    Object value = ((Function) right).apply(event);
                    return test(regex, value);
                };
            } else if (right instanceof Supplier) {
                // function(attribute) matches supplier(variable)
                predicate = event -> {
                    Object regex = ((Function) left).apply(event);
                    Object value = ((Supplier) right).get();
                    return test(regex, value);
                };
            } else {
                throw new WisdomAppValidationException("java.util.function.Function MATCHES %s is not supported", right.getClass().getCanonicalName());
            }
        } else if (left instanceof Supplier) {
            // supplier(variable) matches right
            if (right instanceof String) {
                // supplier(variable) matches constant
                predicate = event -> {
                    Object regex = ((Supplier) left).get();
                    return test(regex, right);
                };
            } else if (right instanceof Function) {
                // supplier(variable) matches function(attribute)
                predicate = event -> {
                    Object regex = ((Supplier) left).get();
                    Object value = ((Function) right).apply(event);
                    return test(regex, value);
                };
            } else if (right instanceof Supplier) {
                // supplier(variable) matches supplier(variable)
                predicate = event -> {
                    Object regex = ((Supplier) left).get();
                    Object value = ((Supplier) right).get();
                    return test(regex, value);
                };
            } else {
                throw new WisdomAppValidationException("java.util.function.Function MATCHES %s is not supported", right.getClass().getCanonicalName());
            }
        } else {
            throw new WisdomAppValidationException("IN %s is not supported", right.getClass().getCanonicalName());
        }
    }

    @Override
    public boolean test(Event event) {
        return this.predicate.test(event);
    }

    private boolean test(Object regex, Object str) {
        if (regex instanceof String && str instanceof String) {
            return Pattern.compile((String) regex).matcher((CharSequence) str).find();
        } else {
            throw new WisdomAppRuntimeException("%s MATCHES %s is not supported", regex.getClass().getCanonicalName(), str.getClass().getCanonicalName());
        }
    }
}
