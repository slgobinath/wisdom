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

import java.util.ArrayList;
import java.util.List;

public class MapStatement implements Statement {

    private List<MapOperator> operators = new ArrayList<>();


    public void addOperator(MapOperator mapper) {
        this.operators.add(mapper);
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        int noOfOperators = operators.size();
        Mapper[] mappers = new Mapper[noOfOperators];
        for (int i = 0; i < noOfOperators; i++) {
            mappers[i] = operators.get(i).build(app, query);
        }
        query.map(mappers);
    }
}
