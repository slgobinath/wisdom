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

package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.window.Window;

import java.time.Duration;

import static com.javahelps.wisdom.core.util.Commons.map;

public class UniqueWindow {

    static {
        ImportsManager.INSTANCE.use(UniqueWindow.class.getPackageName());
    }

    private UniqueWindow() {

    }

    public static Window externalTimeBatch(String uniqueKey, String timestampKey, Duration duration) {

        return Window.create("unique:externalTimeBatch", map("uniqueKey", uniqueKey, "timestampKey", timestampKey, "duration", duration.toMillis()));
    }

    public static Window lengthBatch(String uniqueKey, int length) {
        return Window.create("unique:lengthBatch", map("uniqueKey", uniqueKey, "length", "timestamp", length));
    }

}
