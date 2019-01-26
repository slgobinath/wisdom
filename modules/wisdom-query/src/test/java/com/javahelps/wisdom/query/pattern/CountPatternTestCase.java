/*
 * Copyright (c) 2019, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
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
 * Test WisdomQL for count pattern.
 */
public class CountPatternTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CountPatternTestCase.class);

    @Test
    public void testPattern1() throws InterruptedException {
        LOGGER.info("Test pattern query 1 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM']<2:5> as e1 " +
                "select e1[0].price, e1[1].price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1[0].price", 10.0, "e1[1].price", 20.0),
                map("e1[0].price", 30.0, "e1[1].price", 40.0));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 40.0, "volume", 25));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 30));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testPattern2() throws InterruptedException {
        LOGGER.info("Test pattern query 2 - OUT 2");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM']<2:5> as e1 -> StockStream1[symbol == 'WSO2'] as e2 " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1[0].symbol", "IBM", "e1[0].price", 10.0, "e1[0].volume", 10,
                        "e1[1].symbol", "IBM", "e1[1].price", 20.0, "e1[1].volume", 15,
                        "e2.symbol", "WSO2", "e2.price", 30.0, "e2.volume", 20),
                map("e1[0].symbol", "IBM", "e1[0].price", 40.0, "e1[0].volume", 25,
                        "e1[1].symbol", "IBM", "e1[1].price", 50.0, "e1[1].volume", 30,
                        "e2.symbol", "WSO2", "e2.price", 60.0, "e2.volume", 35));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 40.0, "volume", 25));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 30));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 35));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testPattern3() throws InterruptedException {
        LOGGER.info("Test pattern query 3 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM']<2:5> as e1 -> StockStream1[symbol == 'WSO2'] as e2 " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1[0].symbol", "IBM",
                        "e1[0].price", 10.0,
                        "e1[0].volume", 10,
                        "e1[1].symbol", "IBM",
                        "e1[1].price", 20.0,
                        "e1[1].volume", 15,
                        "e1[2].price", 30.0,
                        "e1[2].symbol", "IBM",
                        "e1[2].volume", 20,
                        "e1[3].price", 40.0,
                        "e1[3].symbol", "IBM",
                        "e1[3].volume", 25,
                        "e1[4].price", 50.0,
                        "e1[4].symbol", "IBM",
                        "e1[4].volume", 30,
                        "e2.symbol", "WSO2",
                        "e2.price", 80.0,
                        "e2.volume", 45));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 40.0, "volume", 25));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 30));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 60.0, "volume", 35));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 70.0, "volume", 40));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 80.0, "volume", 45));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 85.0, "volume", 50));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern4() throws InterruptedException {
        LOGGER.info("Test pattern query 4 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM']<2:5> as e1 -> StockStream1[symbol == 'WSO2'] as e2 " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1[0].symbol", "IBM",
                        "e1[0].price", 10.0,
                        "e1[0].volume", 10,
                        "e1[1].symbol", "IBM",
                        "e1[1].price", 20.0,
                        "e1[1].volume", 15,
                        "e2.symbol", "WSO2",
                        "e2.price", 40.0,
                        "e2.volume", 25));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 40.0, "volume", 25));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 30));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 35));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern5() throws InterruptedException {
        LOGGER.info("Test pattern query 5 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM']<2:5> as e1 -> StockStream1[symbol == 'WSO2'] as e2 " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1[0].symbol", "IBM",
                        "e1[0].price", 10.0,
                        "e1[0].volume", 10,
                        "e1[1].symbol", "IBM",
                        "e1[1].price", 30.0,
                        "e1[1].volume", 20,
                        "e2.symbol", "WSO2",
                        "e2.price", 40.0,
                        "e2.volume", 25));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 40.0, "volume", 25));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern6() throws InterruptedException {
        LOGGER.info("Test pattern query 6 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM']<2:5> as e1 -> StockStream1[symbol == 'WSO2'] as e2 " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 20.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern7() throws InterruptedException {
        LOGGER.info("Test pattern query 7 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM']<0:5> as e1 -> StockStream1[symbol == 'WSO2'] as e2 " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e2.symbol", "WSO2", "e2.price", 10.0, "e2.volume", 10));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 10.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern8() throws InterruptedException {
        LOGGER.info("Test pattern query 8 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[price >= 50 and volume > 100] as e1 -> StockStream1[price <= 40]<0:5> as e2 -> StockStream1[volume <= 70] as e3 " +
                "select e1.symbol, e2[0].symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e2[0].symbol", "GOOGLE", "e3.symbol", "WSO2"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 105));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 21.0, "volume", 81));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 176.6, "volume", 65));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern9() throws InterruptedException {
        LOGGER.info("Test pattern query 9 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[price >= 50 and volume > 100] as e1 -> StockStream1[price <= 40]<:5> as e2 -> StockStream1[volume <= 70] as e3 " +
                "select e1.symbol, e2[0].symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e3.symbol", "GOOGLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 105));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 21.0, "volume", 61));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 176.6, "volume", 65));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern10() throws InterruptedException {
        LOGGER.info("Test pattern query 10 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[price >= 50 and volume > 100] as e1 -> StockStream1[price <= 40]<:5> as e2 -> StockStream1[volume <= 70] as e3 " +
                "select e1.symbol, e2[0].symbol, e2[1].symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e2[0].symbol", "GOOGLE", "e2[1].symbol", "FB",
                "e3.symbol", "WSO2"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 105));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 21.0, "volume", 91));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "FB", "price", 21.0, "volume", 81));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 21.0, "volume", 61));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }
}
