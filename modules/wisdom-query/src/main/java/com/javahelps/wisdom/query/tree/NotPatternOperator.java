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
import com.javahelps.wisdom.core.pattern.TimeConstrainedPattern;
import com.javahelps.wisdom.core.query.Query;

import java.time.Duration;

class NotPatternOperator extends PatternOperator {

    private final PatternOperator operator;
    private final Duration duration;

    NotPatternOperator(PatternOperator operator, Duration duration) {
        this.operator = operator;
        this.duration = duration;
    }

    @Override
    public Pattern build(WisdomApp app, Query query) {
        Pattern pattern = Pattern.not(this.operator.build(app, query));
        if (this.duration != null) {
            pattern = ((TimeConstrainedPattern) pattern).within(this.duration);
        }
        return pattern;
    }
}
