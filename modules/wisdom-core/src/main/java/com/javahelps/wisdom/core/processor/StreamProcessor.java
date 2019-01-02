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

package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.Stream;

/**
 * A {@link Processor} that comes in a {@link com.javahelps.wisdom.core.query.Query} following {@link Stream}.
 * In technical aspect, a {@link com.javahelps.wisdom.core.query.Query} is a linked list of these processors.
 */
public abstract class StreamProcessor implements Processor, Initializable {

    protected String id;
    private Processor previousProcessor;
    private Processor nextProcessor;

    public StreamProcessor(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Processor getNextProcessor() {
        return nextProcessor;
    }

    public void setNextProcessor(Processor nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void init(WisdomApp wisdomApp) {

    }

    @Override
    public void stop() {
        // Do nothing
    }

//    @Override
//    public Checkpoint checkpoint(boolean includeEvents) {
//        Checkpoint checkpoint = Checkpoint.forID(this.id);
//        synchronized (this) {
//            if (this.previousProcessor == null) {
//                checkpoint.add("previousProcessor", null);
//            } else {
//                checkpoint.add("previousProcessor", this.previousProcessor.getId());
//            }
//            if (this.nextProcessor == null) {
//                checkpoint.add("nextProcessor", null);
//            } else {
//                checkpoint.add("nextProcessor", this.nextProcessor.getId());
//            }
//        }
//        return checkpoint;
//    }
//
//    @Override
//    public void restore(Checkpoint checkpoint) {
//        synchronized (this) {
//            this.id = checkpoint.getID();
//            this.previousProcessor = checkpoint.get("previousProcessor");
//            this.nextProcessor = checkpoint.get("nextProcessor");
//        }
//    }
}
