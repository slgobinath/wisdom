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

package com.javahelps.wisdom.core.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class Commons {

    private Commons() {

    }

    public static Map<String, Object> map(Object... entries) {
        int count = entries.length;
        Map<String, Object> map = new HashMap<>(count / 2);
        for (int i = 0; i < count; i += 2) {
            String key = Objects.toString(entries[i]);
            Object value = entries[i + 1];
            if (value instanceof Integer) {
                value = ((Integer) value).longValue();
            } else if (value instanceof Float) {
                value = ((Float) value).doubleValue();
            }
            map.put(key, value);
        }
        return map;
    }

    public static Properties toProperties(Object... entries) {
        int count = entries.length;
        Properties properties = new Properties();
        for (int i = 0; i < count; i += 2) {
            properties.put((String) entries[i], entries[i + 1]);
        }
        return properties;
    }

    public static <T> T getProperty(Map<String, ?> properties, String attribute, int index) {
        T value = (T) properties.get(attribute);
        if (value == null) {
            value = (T) properties.get(String.format("_param_%d", index));
        }
        return value;
    }
}
