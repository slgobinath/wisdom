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

import java.util.Properties;

public class WisdomConfig {

    public static final double DOUBLE_PRECISION = 10_000.0;

    public static final boolean ASYNC_ENABLED = false;

    public static final long STATISTICS_REPORT_FREQUENCY = 60_000L;

    public static final int EVENT_BUFFER_SIZE = 1024;

    public static final String WISDOM_APP_NAME = "WisdomApp";

    public static final String WISDOM_APP_VERSION = "1.0.0";

    public static final Properties EMPTY_PROPERTIES = new Properties();

    private WisdomConfig() {

    }
}
