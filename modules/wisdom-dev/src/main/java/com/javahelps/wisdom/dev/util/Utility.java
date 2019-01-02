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

package com.javahelps.wisdom.dev.util;

import com.google.gson.Gson;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.dev.optimize.multivariate.Constraint;

import java.util.List;
import java.util.Map;

public class Utility {

    private static final Gson gson = new Gson();

    private Utility() {

    }

    public static Constraint[] velocityBound(Constraint... bounds) {
        int length = bounds.length;
        Constraint[] velocityBounds = new Constraint[length];
        for (int i = 0; i < length; i++) {
            double max = Math.abs(bounds[i].getHigh() - bounds[i].getLow());
            double min = -max;
            velocityBounds[i] = new Constraint(min, max);
        }
        return velocityBounds;
    }


    public static int getMinPos(double[] list) {
        int pos = 0;
        double minValue = list[0];

        for (int i = 1; i < list.length; i++) {
            if (list[i] < minValue) {
                pos = i;
                minValue = list[i];
            }
        }

        return pos;
    }

    /**
     * Convert JSON string to java.util.Map.
     *
     * @param jsonString
     * @return
     */
    public static Map<String, Object> toMap(String jsonString) {
        return gson.fromJson(jsonString, Map.class);
    }

    /**
     * Convert given java.util.Map to JSON string.
     *
     * @param data
     * @return
     */
    public static String toJson(Map<String, ?> data) {
        return gson.toJson(data);
    }

    /**
     * Create a JSON string containing all attributes of the event.
     *
     * @param event Wisdom event
     * @return JSON string
     */
    public static String toJson(Event event) {
        return gson.toJson(event.getData());
    }

    /**
     * Create a JSON string array containing attributes of all events.
     *
     * @param events a list of Wisdom events
     * @return JSON string
     */
    public static String toJson(List<Event> events) {
        int length = events.size();
        Map<String, Object>[] dataArray = new Map[length];
        for (int i = 0; i < length; i++) {
            dataArray[i] = events.get(i).getData();
        }
        return gson.toJson(dataArray);
    }
}
