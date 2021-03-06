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

package com.javahelps.wisdom.core.processor.partition;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Attribute;
import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.window.Window;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.javahelps.wisdom.core.util.Commons.map;

/**
 * Test the {@link com.javahelps.wisdom.core.processor.PartitionProcessor} of Wisdom.
 */
public class PartitionTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionTestCase.class);

    @Test
    public void testPartition1() throws InterruptedException {
        LOGGER.info("Test partition 1 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .partitionByAttr("symbol")
                .window(Window.lengthBatch(2))
                .aggregate(Operator.SUM("price", "price"))
                .select("symbol", "price")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 110.0),
                map("symbol", "ORACLE", "price", 150.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 20));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 80.0, "volume", 25));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testPartition2() throws InterruptedException {
        LOGGER.info("Test partition 2 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .partitionByAttr("symbol", "volume")
                .window(Window.lengthBatch(2))
                .aggregate(Operator.SUM("price", "price"))
                .select("symbol", "price")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("symbol",
                "IBM", "price", 110.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 20));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 60.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 80.0, "volume", 25));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPartition3() throws InterruptedException {
        LOGGER.info("Test partition 3 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .partitionByAttr("symbol")
                .select("symbol", "price")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 50.0),
                map("symbol", "WSO2", "price", 70.0),
                map("symbol", "WSO2", "price", 60.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 70.0, "volume", 20));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 3, callback.getEventCount());
    }

    @Test
    public void testPartition4() throws InterruptedException {
        LOGGER.info("Test partition 4 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .filter(Operator.LESS_THAN(Attribute.of("price"), 700.0))
                .partitionByAttr("symbol")
                .aggregate(Operator.SUM("price", "price"))
                .select("symbol", "price", "volume")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream"/*,
                map("symbol", "IBM", "price", 50.0),
                map("symbol", "WSO2", "price", 70.0),
                map("symbol", "WSO2", "price", 60.0)*/);

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 70005.6, "volume", 20));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 30));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 75.6, "volume", 40));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 3, callback.getEventCount());
    }


    @Test
    public void testPartition5() throws InterruptedException {
        LOGGER.info("Test partition 5 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .partitionByAttr("symbol", "group")
                .window(Window.lengthBatch(2))
                .aggregate(Operator.SUM("price", "price"))
                .select("symbol", "price")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "group", "GOOGLE"));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "group", "IBM"));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 60.0, "group", "ORACLE"));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 80.0, "group", "GOOGLE"));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPartition6() throws InterruptedException {
        LOGGER.info("Test partition 6 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .partitionByVal("symbol", "group")
                .window(Window.lengthBatch(2))
                .aggregate(Operator.SUM("price", "price"))
                .select("symbol", "price")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("symbol",
                "IBM", "price", 130.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "group", "GOOGLE"));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "group", "IBM"));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 60.0, "group", "ORACLE"));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 80.0, "group", "GOOGLE"));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

}
