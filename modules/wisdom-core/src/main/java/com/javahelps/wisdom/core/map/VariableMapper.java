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

package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.util.Commons;
import com.javahelps.wisdom.core.variable.Variable;

import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.VARIABLE;

@WisdomExtension("variable")
public class VariableMapper extends Mapper implements Variable.OnUpdateListener<Comparable> {

    private final String varName;
    private Variable<Comparable> variable;
    private Comparable value;

    public VariableMapper(String attrName, Map<String, ?> properties) {
        super(attrName, properties);
        this.varName = Commons.getProperty(properties, VARIABLE, 0);
        if (this.varName == null) {
            throw new WisdomAppValidationException("Required property %s for Variable mapper not found", VARIABLE);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void init(WisdomApp wisdomApp) {
        this.variable = wisdomApp.getVariable(this.varName);
        this.value = this.variable.get();
        this.variable.addOnUpdateListener(this);
    }

    @Override
    public void stop() {
        this.variable.removeOnUpdateListener(this);
    }

    @Override
    public Event map(Event event) {
        return event.set(this.attrName, this.value);
    }

    @Override
    public void update(Comparable value) {
        this.value = value;
    }
}
