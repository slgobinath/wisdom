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

package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.time.EventBasedTimestampGenerator;

/**
 * InputHandler to send events into a stream.
 */
public class InputHandler {

    private final Processor processor;
    private final WisdomApp wisdomApp;
    private EventBasedTimestampGenerator timestampGenerator;
    private String playbackTimestamp;

    public InputHandler(Processor processor, WisdomApp wisdomApp) {
        this.processor = processor;
        this.wisdomApp = wisdomApp;
        if (wisdomApp.getContext().isPlaybackEnabled()) {
            this.timestampGenerator = (EventBasedTimestampGenerator) wisdomApp.getContext().getTimestampGenerator();
            this.playbackTimestamp = wisdomApp.getContext().getPlaybackAttribute();
        }
    }

    public void send(Event event) {
        if (this.timestampGenerator != null) {
            Object timestamp = event.get(this.playbackTimestamp);
            if (timestamp != null && timestamp instanceof Number) {
                this.timestampGenerator.setCurrentTimestamp(((Number) timestamp).longValue());
            }
        }
        try {
            this.processor.process(event);
        } catch (WisdomAppRuntimeException ex) {
            this.wisdomApp.handleException(ex);
        }
    }
}
