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
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.core.window.Window;
import com.javahelps.wisdom.query.util.Utility;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WindowStatement implements Statement {

    private final String name;
    private String type;
    private List<KeyValueElement> keyValueElements = new ArrayList<>();

    public WindowStatement(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void addProperty(KeyValueElement element) {
        this.keyValueElements.add(element);
    }

    @Override
    public void addTo(WisdomApp app, Query query) {
        String namespace = type == null ? name : name + ":" + type;
        Map<String, Object> properties = Utility.toProperties(app, this.keyValueElements);
        Window window = Window.create(namespace, properties);
        query.window(window);
    }
}
