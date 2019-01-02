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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.javahelps.wisdom.query.util.Constants.ANNOTATION.CONFIG_ANNOTATION;

public abstract class Definition {

    private final String name;
    protected Properties configuration;
    protected List<Annotation> annotations = new ArrayList<>();

    protected Definition(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public abstract void define(WisdomApp app);

    public final void addAnnotation(ParserRuleContext ctx, Annotation annotation) {
        if (CONFIG_ANNOTATION.equals(annotation.getName())) {
            if (this.configuration != null) {
                throw new WisdomParserException(ctx, "@" + CONFIG_ANNOTATION + " is already defined for " + getName());
            }
            this.configuration = annotation.getProperties();
        } else {
            this.verifyAnnotation(ctx, annotation);
            this.annotations.add(annotation);
        }
    }

    protected abstract void verifyAnnotation(ParserRuleContext ctx, Annotation annotation);
}
