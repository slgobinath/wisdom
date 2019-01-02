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

import com.javahelps.wisdom.query.antlr.WisdomParserException;
import spark.ExceptionHandler;
import spark.Request;
import spark.Response;

import static com.javahelps.wisdom.dev.util.Constants.HTTP_BAD_REQUEST;
import static com.javahelps.wisdom.dev.util.Constants.MEDIA_TEXT_PLAIN;


public class WisdomParserExceptionHandler implements ExceptionHandler<WisdomParserException> {

    @Override
    public void handle(WisdomParserException exception, Request request, Response response) {
        response.status(HTTP_BAD_REQUEST);
        response.type(MEDIA_TEXT_PLAIN);
        response.body(exception.getMessage());
    }
}
