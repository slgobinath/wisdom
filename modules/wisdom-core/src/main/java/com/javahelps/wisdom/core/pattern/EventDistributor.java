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

package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.event.Event;

import java.util.*;

/**
 * Created by gobinath on 7/4/17.
 */
class EventDistributor {

    private Map<String, Set<Pattern>> patternsMap = new HashMap<>();
    private List<Pattern> patternList = new ArrayList<>();

    public void add(Pattern pattern) {
        if (pattern == null) {
            return;
        }
        pattern.streamIds.forEach(id -> {
            Set<Pattern> patterns = patternsMap.get(id);
            if (patterns == null) {
                patterns = new LinkedHashSet<>();
                patternsMap.put(id, patterns);
            }
            patterns.add(pattern);
        });
        this.patternList.add(pattern);
    }

    public void process(Event event) {
        for (Pattern pattern : this.patternsMap.get(event.getStream().getId())) {
            if (pattern.isAccepting()) {
                pattern.process(event);
                if (pattern.isConsumed()) {
                    // Consumed the event
                    break;
                }
            }
        }
    }
}
