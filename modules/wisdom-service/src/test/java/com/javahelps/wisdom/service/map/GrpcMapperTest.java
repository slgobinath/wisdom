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

package com.javahelps.wisdom.service.map;

import com.google.gson.Gson;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.service.TestWisdomService;
import com.javahelps.wisdom.service.gprc.WisdomGrpc;
import com.javahelps.wisdom.service.gprc.WisdomGrpcService;
import com.javahelps.wisdom.service.util.TestUtil;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.javahelps.wisdom.core.util.Commons.map;

public class GrpcMapperTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestWisdomService.class);
    private static Server server;

    static {
        ImportsManager.INSTANCE.use(GrpcMapper.class);
    }

    @BeforeClass
    public static void init() throws IOException {
        Gson gson = new Gson();
        // Create a new server to listen on port 8080
        server = ServerBuilder.forPort(8080)
                .addService(new WisdomGrpc.WisdomImplBase() {
                    @Override
                    public void send(WisdomGrpcService.Event request, StreamObserver<WisdomGrpcService.Event> responseObserver) {
                        Map<String, Object> map = gson.fromJson(request.getData(), Map.class);
                        double price = ((Number) map.get("price")).doubleValue();
                        int volume = ((Number) map.get("volume")).intValue();

                        WisdomGrpcService.Event reply = WisdomGrpcService.Event.newBuilder().setData(gson.toJson(Map.of("accuracy", price * volume))).build();
                        // Use responseObserver to send a single response back
                        responseObserver.onNext(reply);

                        // When you are done, you must call onCompleted.
                        responseObserver.onCompleted();
                    }
                })
                .build();

        // Start the server
        server.start();
    }

    @AfterClass
    public static void clean() {
        server.shutdown();
    }

    @Test
    public void testGrpcMapper1() throws InterruptedException {
        LOGGER.info("Test Grpc mapper 1 - OUT 3");

        WisdomApp app = new WisdomApp();
        app.defineStream("StockStream");
        app.defineStream("OutputStream");

        app.defineQuery("query1")
                .from("StockStream")
                .map(new GrpcMapper("accuracy", map("endpoint", "localhost:8080", "select", "accuracy")))
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, app, "OutputStream",
                map("symbol", "IBM", "price", 50.0, "volume", 10, "accuracy", 500.0),
                map("symbol", "WSO2", "price", 60.0, "volume", 15, "accuracy", 900.0),
                map("symbol", "ORACLE", "price", 70.0, "volume", 20, "accuracy", 1400.0));

        app.start();

        InputHandler stockStreamInputHandler = app.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 20));

        app.shutdown();
        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 3, callback.getEventCount());
    }
}
