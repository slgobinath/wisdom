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

package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

abstract class WrappingPattern extends Pattern {

    private final List<Pattern> children = new LinkedList<>();
    private boolean resetInProcess = false;

    WrappingPattern(String patternId, Pattern child, Pattern... children) {
        super(patternId);
        // Create the list
        this.children.add(child);
        for (Pattern pattern : children) {
            this.children.add(pattern);
        }
        // Set globalResetAction action and get all stream ids
        for (Pattern pattern : this.children) {
            pattern.globalResetAction = this::globalReset;
            this.streamIds.addAll(pattern.streamIds);
        }
    }

    @Override
    public void init(WisdomApp wisdomApp) {
        // Initialize timestampGenerator
        super.init(wisdomApp);
        for (Pattern pattern : this.children) {
            pattern.init(wisdomApp);
            pattern.streamIds.forEach(streamId -> {
                wisdomApp.getStream(streamId).removeProcessor(pattern);
                wisdomApp.getStream(streamId).addProcessor(this);
            });
        }
    }

    protected void globalReset() {
        if (resetInProcess) {
            return;
        }
        try {
            resetInProcess = true;
            this.reset();
            this.globalResetAction.execute();
        } finally {
            resetInProcess = false;
        }
    }

    @Override
    public final void reset() {
        for (Pattern pattern : this.children) {
            pattern.reset();
        }
    }

    @Override
    public final Pattern filter(Predicate<Event> predicate) {
        throw new WisdomAppValidationException("%s cannot have filter", this.getClass().getSimpleName());
    }
}
