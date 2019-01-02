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
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.dev.util.Utility;
import com.javahelps.wisdom.service.WisdomService;
import com.javahelps.wisdom.service.gprc.WisdomGrpc;
import com.javahelps.wisdom.service.gprc.WisdomGrpcService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.javahelps.wisdom.dev.util.Constants.PORT;

@WisdomExtension("grpc")
public class GrpcSource extends Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomService.class);

    private final Gson gson = new Gson();
    private int port;
    private InputHandler inputHandler;
    private String streamId;
    private Server server;

    public GrpcSource(Map<String, ?> properties) {
        super(properties);
        Number port = (Number) properties.get(PORT);
        if (port == null) {
            throw new WisdomAppValidationException("Expected property %s for gRpc source is not found", PORT);
        }
        this.port = port.intValue();
    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {
        this.streamId = streamId;
        this.inputHandler = wisdomApp.getInputHandler(streamId);
        this.server = ServerBuilder.forPort(this.port)
                .addService(new WisdomGprcSourceImpl())
                .build();
    }

    @Override
    public synchronized void start() {
        if (this.server != null) {
            LOGGER.info("Start gRpc source at {}", this.port);
            try {
                this.server.start();
            } catch (IOException e) {
                throw new WisdomAppRuntimeException("Failed to start gRpc source at " + this.port, e);
            }
        }
    }

    @Override
    public synchronized void stop() {
        // Handled by server
        if (this.server != null) {
            this.server.shutdown();
        }
    }

    private class WisdomGprcSourceImpl extends WisdomGrpc.WisdomImplBase {
        @Override
        public StreamObserver<WisdomGrpcService.Event> feed(StreamObserver<WisdomGrpcService.Event> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(WisdomGrpcService.Event event) {
                    inputHandler.send(EventGenerator.generate(Utility.toMap(event.getData())));
                }

                @Override
                public void onError(Throwable throwable) {
                    throw new WisdomAppRuntimeException("Error in Rpc source", throwable);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(WisdomGrpcService.Event.newBuilder()
                            .setData("{\"status\": \"completed\"}")
                            .build());
                }
            };
        }

        @Override
        public void send(WisdomGrpcService.Event event, StreamObserver<WisdomGrpcService.Event> responseObserver) {
            inputHandler.send(EventGenerator.generate(Utility.toMap(event.getData())));
            responseObserver.onNext(WisdomGrpcService.Event.newBuilder().setData("{\"status\": \"accepted\"}").build());
            responseObserver.onCompleted();
        }
    }
}
