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

package com.javahelps.wisdom.extensions.ml.onehot;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.operand.WisdomArray;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class ToOneHotMapperTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ToOneHotMapperTest.class);

    static {
        ImportsManager.INSTANCE.use(ToOneHotMapper.class);
    }

    @Test
    public void testToOneHotMapper1() throws InterruptedException {
        LOGGER.info("Test ToOneHot mapper 1 - OUT 5");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .map(Mapper.create("toOneHot", "symbol", Map.of("attr", "symbol", "items", WisdomArray.of("IBM", "WSO2", "ORACLE"))))
                .select("symbol")
                .insertInto("OutputStream");

        AtomicInteger count = new AtomicInteger();
        wisdomApp.addCallback("OutputStream", events -> {
            LOGGER.info(Arrays.toString(events));
            int received = count.addAndGet(events.length);
            if (received == 1) {
                Assert.assertArrayEquals("Incorrect one hot mapping", new int[]{1, 0, 0}, (int[]) events[0].get("symbol"));
            } else {
                Assert.assertArrayEquals("Incorrect one hot mapping", new int[]{0, 0, 1}, (int[]) events[0].get("symbol"));
            }
        });

        wisdomApp.start();

        InputHandler inputHandler = wisdomApp.getInputHandler("StockStream");
        inputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 15.0));
        inputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 25.0));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, count.get());
    }
}
