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

package com.javahelps.wisdom.core.exception;

/**
 * Exception during {@link com.javahelps.wisdom.core.WisdomApp} execution.
 */
public class WisdomAppRuntimeException extends RuntimeException {

    public WisdomAppRuntimeException(String message) {
        super(message);
    }

    public WisdomAppRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public WisdomAppRuntimeException(String message, Object... args) {
        this(String.format(message, args));
    }

    public WisdomAppRuntimeException(Throwable throwable) {
        super(throwable);
    }
}
