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

package com.javahelps.wisdom.service.source;

import com.google.gson.Gson;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.service.WisdomService;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.util.Map;
import java.util.function.Function;

import static com.javahelps.wisdom.dev.util.Constants.*;

@WisdomExtension("http")
public class HTTPSource extends Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomService.class);

    private final Gson gson = new Gson();
    private final Function<String, Map<String, Object>> mapper;
    private String endpoint;
    private InputHandler inputHandler;
    private String streamId;

    public HTTPSource(Map<String, ?> properties) {
        super(properties);
        this.endpoint = (String) properties.get(ENDPOINT);
        String mapping = (String) properties.get(MAPPING);
        if (JSON.equalsIgnoreCase(mapping)) {
            this.mapper = body -> this.gson.fromJson(body, Map.class);
        } else {
            throw new WisdomAppValidationException("Unsupported mapping for HTTP source: %s", mapping);
        }
    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {
        if (this.endpoint == null) {
            this.endpoint = streamId;
        }
        this.streamId = streamId;
        this.inputHandler = wisdomApp.getInputHandler(streamId);
    }

    @Override
    public void start() {
        Spark.post("/WisdomApp/" + this.endpoint, MEDIA_APPLICATION_JSON, this::send);
    }

    @Override
    public void stop() {
        // Handled by server
    }

    private Response send(Request request, Response response) {

        Map<String, Object> data = this.mapper.apply(request.body());
        LOGGER.debug("Received event for {}:{}", this.streamId, data);
        if (inputHandler != null) {
            inputHandler.send(EventGenerator.generate(data));
            response.type(MEDIA_TEXT_PLAIN);
            response.status(HTTP_ACCEPTED);
        } else {
            throw new WisdomServiceException(
                    String.format("The stream %s is neither defined nor not an input stream", streamId));
        }
        return response;
    }
}
