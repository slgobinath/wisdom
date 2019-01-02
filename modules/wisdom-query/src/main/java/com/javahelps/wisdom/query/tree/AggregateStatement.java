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
import com.javahelps.wisdom.core.operator.AggregateOperator;
import com.javahelps.wisdom.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public class AggregateStatement implements Statement {

    private List<AggregateOperatorNode> operators = new ArrayList<>();


    public void addOperator(AggregateOperatorNode operator) {
        this.operators.add(operator);
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        int noOfOperators = operators.size();
        AggregateOperator[] aggregateOperators = new AggregateOperator[noOfOperators];
        for (int i = 0; i < noOfOperators; i++) {
            aggregateOperators[i] = this.operators.get(i).build(app, query);
        }
        query.aggregate(aggregateOperators);
    }
}
