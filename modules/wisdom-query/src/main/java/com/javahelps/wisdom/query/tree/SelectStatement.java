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
import com.javahelps.wisdom.core.event.Index;
import com.javahelps.wisdom.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public class SelectStatement implements Statement {

    private final List<String> attributes = new ArrayList<>();
    private final List<Integer> indices = new ArrayList<>();

    public void addAttribute(String attribute) {
        this.attributes.add(attribute);
    }

    public void addIndex(int index) {
        this.indices.add(index);
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        if (!this.attributes.isEmpty()) {
            query.select(attributes.toArray(new String[0]));
        } else if (!this.indices.isEmpty()) {
            final int noOfSize = indices.size();
            int[] values = new int[noOfSize];
            for (int i = 0; i < noOfSize; i++) {
                values[i] = indices.get(i);
            }
            query.select(Index.of(values));
        }
    }
}
