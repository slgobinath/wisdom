/*
 * Copyright (c) 2019, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
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
import com.javahelps.wisdom.core.pattern.Pattern;
import com.javahelps.wisdom.core.query.Query;

import java.time.Duration;

public class PatternOperator implements OperatorElement {

    private String streamName;
    private LogicalOperator logicalOperator;
    private Long minCount;
    private Long maxCount;
    private String alias;

    protected PatternOperator() {

    }

    public static PatternOperator create(String streamName, LogicalOperator logicalOperator, String alias) {
        PatternOperator operator = new PatternOperator();
        operator.streamName = streamName;
        operator.logicalOperator = logicalOperator;
        operator.alias = alias != null ? alias : streamName;
        return operator;
    }

    public static PatternOperator every(PatternOperator operator) {
        return new EveryPatternOperator(operator);
    }

    public static PatternOperator not(PatternOperator operator, Duration duration) {
        return new NotPatternOperator(operator, duration);
    }

    public static PatternOperator and(PatternOperator left, PatternOperator right) {
        return new AndPatternOperator(left, right);
    }

    public static PatternOperator or(PatternOperator left, PatternOperator right) {
        return new OrPatternOperator(left, right);
    }

    public static PatternOperator follows(PatternOperator first, PatternOperator second, Duration duration) {
        return new FollowsPatternOperator(first, second, duration);
    }

    public void setMaxCount(Long maxCount) {
        this.maxCount = maxCount;
    }

    public void setMinCount(Long minCount) {
        this.minCount = minCount;
    }

    public Pattern build(WisdomApp app, Query query) {

        Pattern pattern = query.definePattern(streamName, alias);
        if (logicalOperator != null) {
            logicalOperator.setReadFromSupplier(true);
            pattern = pattern.filter(logicalOperator.build(app, query));
        }
        if (minCount != null && maxCount != null) {
            pattern = pattern.times(minCount, maxCount);
        } else if (minCount != null) {
            pattern = pattern.times(minCount);
        } else if (maxCount != null) {
            pattern = pattern.times(maxCount);
        }
        return pattern;
    }
}
