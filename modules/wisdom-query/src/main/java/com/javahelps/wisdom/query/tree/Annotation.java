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

import java.util.Properties;

public class Annotation {

    private final String name;
    private final Properties properties;

    public Annotation(String name) {
        this(name, new Properties());
    }

    public Annotation(String name, Properties properties) {
        this.name = name;
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public <T> T getProperty(String key) {
        return (T) this.properties.get(key);
    }

    public <T> void setProperty(String key, T value) {
        this.properties.put(key, value);
    }

    public boolean hasProperty(String key) {
        return this.properties.containsKey(key);
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return String.format("@%s%s", this.name, this.properties);
    }
}
