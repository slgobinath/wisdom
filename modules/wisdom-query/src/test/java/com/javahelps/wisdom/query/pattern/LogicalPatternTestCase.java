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
 * Test logical patterns of Wisdom which includes AND, OR & NOT.
 */
public class LogicalPatternTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalPatternTestCase.class);

    @Test
    public void testPattern1() throws InterruptedException {
        LOGGER.info("Test pattern query 1 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 and StockStream2[symbol == 'WSO2'] as e2 -> StockStream3[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern2() throws InterruptedException {
        LOGGER.info("Test pattern query 2 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 and StockStream2[symbol == 'WSO2'] as e2 -> StockStream3[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern3() throws InterruptedException {
        LOGGER.info("Test pattern query 3 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 and StockStream2[symbol == 'WSO2'] as e2 -> StockStream3[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern4() throws InterruptedException {
        LOGGER.info("Test pattern query 4 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 and StockStream2[symbol == 'WSO2'] as e2 -> StockStream3[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern5() throws InterruptedException {
        LOGGER.info("Test pattern query 5 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 or StockStream2[symbol == 'WSO2'] as e2 -> StockStream3[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern6() throws InterruptedException {
        LOGGER.info("Test pattern query 6 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 or StockStream2[symbol == 'WSO2'] as e2 -> StockStream3[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern7() throws InterruptedException {
        LOGGER.info("Test pattern query 7 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 or StockStream2[symbol == 'WSO2'] as e2 -> StockStream3[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern8() throws InterruptedException {
        LOGGER.info("Test pattern query 8 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 or StockStream2[symbol == 'WSO2'] as e2 -> StockStream3[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());

    }

    @Test
    public void testPattern9() {
        LOGGER.info("Test pattern query 9 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream StockStream4; " +
                "def stream OutputStream; " +
                "" +
                "from (" +
                "   (StockStream1[symbol == 'IBM'] as e1 and StockStream2[symbol == 'WSO2'] as e2) " +
                "       or StockStream3[symbol == 'ORACLE'] as e3) " +
                "-> StockStream4[symbol == 'MICROSOFT'] as e4 " +
                "select e1.symbol, e2.symbol, e3.symbol, e4.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE", "e4.symbol", "MICROSOFT"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));
        wisdomApp.send("StockStream4", EventGenerator.generate("symbol", "MICROSOFT", "price", 60.0, "volume", 10));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern10() {
        LOGGER.info("Test pattern query 10 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream OutputStream; " +
                "" +
                "from not StockStream1[symbol == 'IBM'] -> StockStream2[symbol == 'WSO2'] as e2 " +
                "select e2.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e2.symbol", "WSO2"));

        wisdomApp.start();

        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern11() {
        LOGGER.info("Test pattern query 11 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream OutputStream; " +
                "" +
                "from not StockStream1[symbol == 'IBM'] -> StockStream2[symbol == 'WSO2'] as e2 " +
                "select e2.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern12() throws InterruptedException {
        LOGGER.info("Test pattern query 12 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 -> not (StockStream2[symbol == 'WSO2']) within time.sec(1) " +
                "select e1.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM"));

        wisdomApp.start();


        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));

        Thread.sleep(1100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern13() throws InterruptedException {
        LOGGER.info("Test pattern query 13 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 -> not StockStream2[symbol == 'WSO2'] -> StockStream3[price >= e1.price] as e3 " +
                "select e1.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e3.symbol", "WSO2"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern14() throws InterruptedException {
        LOGGER.info("Test pattern query 14 - OUT 0");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 -> not StockStream2[symbol == 'WSO2'] -> StockStream3[price >= e1.price] as e3 " +
                "select e1.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 55.0, "volume", 12));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern15() {
        LOGGER.info("Test pattern query 15 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream StockStream4; " +
                "def stream OutputStream; " +
                "" +
                "from ((StockStream1[symbol == 'IBM'] as e1 and StockStream2[symbol == 'WSO2'] as e2) or StockStream3[symbol == 'ORACLE'] as e3) -> StockStream4[symbol == 'MICROSOFT'] as e4 " +
                "select e1.symbol, e2.symbol, e3.symbol, e4.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e4.symbol", "MICROSOFT"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream4", EventGenerator.generate("symbol", "MICROSOFT", "price", 60.0, "volume", 10));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern16() {
        LOGGER.info("Test pattern query 16 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream StockStream4; " +
                "def stream OutputStream; " +
                "" +
                "from ((StockStream1[symbol == 'IBM'] as e1 and StockStream2[symbol == 'WSO2'] as e2) or StockStream3[symbol == 'ORACLE'] as e3) -> StockStream4[symbol == 'MICROSOFT'] as e4 " +
                "select e1.symbol, e2.symbol, e3.symbol, e4.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e3.symbol", "ORACLE", "e4.symbol", "MICROSOFT"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));
        wisdomApp.send("StockStream4", EventGenerator.generate("symbol", "MICROSOFT", "price", 60.0, "volume", 10));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern17() throws InterruptedException {
        LOGGER.info("Test pattern 17 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 and StockStream2[symbol == 'WSO2'] as e2 and StockStream3[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern18() throws InterruptedException {
        LOGGER.info("Test pattern 18 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from (StockStream1[symbol == 'IBM'] as e1 and StockStream2[symbol == 'WSO2'] as e2) or StockStream3[symbol == 'ORACLE'] as e3 " +
                "select e1.symbol, e2.symbol, e3.symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern19() throws InterruptedException {
        LOGGER.info("Test pattern query 19 - OUT 1");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream1; " +
                "def stream StockStream2; " +
                "def stream StockStream3; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream1[symbol == 'IBM'] as e1 -> not StockStream2[symbol == 'WSO2'] -> StockStream3[price >= e1.price] as e3 " +
                "select e1.price, e3.price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1.price", 51.0, "e3.price", 61.0));

        wisdomApp.start();


        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 55.0, "volume", 12));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 51.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 56.0, "volume", 12));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "MICROSOFT", "price", 61.0, "volume", 15));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }
}
