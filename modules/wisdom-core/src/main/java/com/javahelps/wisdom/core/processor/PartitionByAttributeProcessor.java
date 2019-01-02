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

import com.javahelps.wisdom.core.event.Event;

import java.util.Objects;

public class PartitionByAttributeProcessor extends PartitionProcessor {

    public PartitionByAttributeProcessor(String id, String... attributes) {
        super(id, attributes);
    }

    protected String calculateKey(Event event) {
        if (this.attributes.length == 1) {
            return Objects.toString(event.get(this.attributes[0]));
        } else {
            StringBuilder builder = new StringBuilder();
            for (String attribute : this.attributes) {
                builder.append(Objects.toString(event.get(attribute)));
            }
            return builder.toString();
        }
    }

    @Override
    public Processor copy() {
        PartitionProcessor processor = new PartitionByAttributeProcessor(this.id, this.attributes);
        processor.setNextProcessor(getNextProcessor().copy());
        return processor;
    }
}
