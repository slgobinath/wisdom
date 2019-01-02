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
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.query.util.Utility;
import org.antlr.v4.runtime.ParserRuleContext;

import static com.javahelps.wisdom.query.util.Constants.ANNOTATION.*;

public class StreamDefinition extends Definition {

    public StreamDefinition(String name) {
        super(name);
    }

    @Override
    protected void verifyAnnotation(ParserRuleContext ctx, Annotation annotation) {
        if (SINK_ANNOTATION.equals(annotation.getName())) {
            Utility.verifyAnnotation(ctx, annotation, SINK_ANNOTATION, TYPE);
        } else if (SOURCE_ANNOTATION.equals(annotation.getName())) {
            Utility.verifyAnnotation(ctx, annotation, SOURCE_ANNOTATION, TYPE);
        }
    }

    @Override
    public void define(WisdomApp app) {
        if (this.configuration == null) {
            app.defineStream(this.getName());
        } else {
            app.defineStream(this.getName(), this.configuration);
        }
        for (Annotation annotation : this.annotations) {
            if (SINK_ANNOTATION.equals(annotation.getName())) {
                app.addSink(getName(), Sink.create(annotation.getProperty(TYPE), Utility.toMap(annotation.getProperties())));
            } else if (SOURCE_ANNOTATION.equals(annotation.getName())) {
                app.addSource(getName(), Source.create(annotation.getProperty(TYPE), Utility.toMap(annotation.getProperties())));
            }
        }
    }
}
