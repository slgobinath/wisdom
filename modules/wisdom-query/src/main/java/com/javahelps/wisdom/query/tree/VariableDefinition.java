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
import com.javahelps.wisdom.query.antlr.WisdomParserException;
import org.antlr.v4.runtime.ParserRuleContext;

public class VariableDefinition extends Definition {

    private final Comparable value;

    public VariableDefinition(String name, Comparable value) {
        super(name);
        this.value = value;
    }

    public Comparable getValue() {
        return value;
    }

    @Override
    protected void verifyAnnotation(ParserRuleContext ctx, Annotation annotation) {
        throw new WisdomParserException(ctx, String.format("@%s is not supported for Variable", annotation.getName()));
    }

    @Override
    public void define(WisdomApp app) {
        if (this.configuration == null) {
            app.defineVariable(this.getName(), this.value);
        } else {
            app.defineVariable(this.getName(), this.value, this.configuration);
        }
    }
}
