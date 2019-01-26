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

package com.javahelps.wisdom.query.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.query.TestUtil;
import com.javahelps.wisdom.query.WisdomCompiler;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.javahelps.wisdom.core.util.Commons.map;

/**
 * Test general patterns of Wisdom.
 */
public class PatternTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(PatternTestCase.class);

    @Test
    public void testPattern1() throws InterruptedException {
        LOGGER.info("Test pattern query 1 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] -> StockStream2[symbol == 'WSO2'] as e2 -> StockStream2[symbol == 'ORACLE'] as e3 " +
                "select StockStream1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map
                ("StockStream1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern2() throws InterruptedException {
        LOGGER.info("Test pattern query 2 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 -> StockStream2[symbol == 'WSO2'] as e2 -> StockStream2[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern3() throws InterruptedException {
        LOGGER.info("Test pattern query 3 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 -> StockStream2[symbol == 'WSO2'] as e2 -> StockStream2[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern4() throws InterruptedException {
        LOGGER.info("Test pattern query 4 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 -> StockStream2[symbol == 'WSO2'] as e2 -> StockStream2[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern5() throws InterruptedException {
        LOGGER.info("Test pattern query 5 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 -> StockStream1[symbol == 'IBM'] as e2 " +
                "select * " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern6() throws InterruptedException {
        LOGGER.info("Test pattern 6 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 -> StockStream1[symbol == 'IBM'] as e2 " +
                "select * " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map
                ("e1.symbol", "IBM", "e2.symbol", "IBM", "e1.price", 50.0, "e2.price",
                        55.0,
                        "e1.volume", 10, "e2.volume", 15));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 55.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern7() throws InterruptedException {
        LOGGER.info("Test pattern query 7 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 " +
                "select * " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map
                        ("e1.symbol", "IBM", "e1.price", 50.0, "e1.volume", 10)
                , map("e1.symbol", "IBM", "e1.price", 55.0, "e1.volume", 15));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 55.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }
}
