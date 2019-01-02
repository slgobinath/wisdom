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

package com.javahelps.wisdom.manager.util;

public class Constants {

    public static final String ARTIFACTS_DIR = "artifacts";
    public static final String CONF_DIR = "conf";
    public static final String ARTIFACTS_CONFIG_FILE = "artifacts.yaml";
    public static final String WISDOM_HOME = "WISDOM_HOME";
    public static final int HTTP_CREATED = 201;
    public static final int MINIMUM_PRIORITY_THRESHOLD = 7;
    public static final double MINIMUM_THROUGHPUT_THRESHOLD = 0.0;
    public static final double MAXIMUM_THROUGHPUT_THRESHOLD = 0.00000001;
    public static final int THROUGHPUT_WINDOW_LENGTH = 30;
    public static long STATISTIC_MONITOR_INITIAL_DELAY = 900000L;   // 15 minutes
    public static long STATISTIC_MONITOR_PERIOD = 900000L;   // 15 minutes

    private Constants() {

    }
}
