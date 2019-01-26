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

package com.javahelps.wisdom.core.processor.pattern;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Attribute;
import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.pattern.Pattern;
import com.javahelps.wisdom.core.query.Query;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.javahelps.wisdom.core.util.Commons.map;

/**
 * Created by gobinath on 6/28/17.
 */
public class CountPatternTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CountPatternTestCase.class);

    @Test
    public void testPattern1() throws InterruptedException {
        LOGGER.info("Test pattern 1 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        Query query = wisdomApp.defineQuery("query1");

        // e1<2:5>
        Pattern pattern = query.definePattern("StockStream1", "e1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);

        query.from(pattern)
                .select("e1[0].price", "e1[1].price")
                .insertInto("OutputStream");

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
        LOGGER.info("Test pattern 2 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        Query query = wisdomApp.defineQuery("query1");
        // e1 < 2:5 > ->e2
        Pattern e1 = query.definePattern("StockStream1", "e1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);
        Pattern e2 = query.definePattern("StockStream1", "e2")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        query.from(pattern)
                .insertInto("OutputStream");

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
        LOGGER.info("Test pattern 3 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        Query query = wisdomApp.defineQuery("query1");

        // e1<2:5> -> e2
        Pattern e1 = query.definePattern("StockStream1", "e1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);
        Pattern e2 = query.definePattern("StockStream1", "e2")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        query.from(pattern)
                .insertInto("OutputStream");

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
        LOGGER.info("Test pattern 4 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        Query query = wisdomApp.defineQuery("query1");

        // e1<2:5> -> e2
        Pattern e1 = query.definePattern("StockStream1", "e1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);
        Pattern e2 = query.definePattern("StockStream1", "e2")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        query.from(pattern)
                .insertInto("OutputStream");

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
        LOGGER.info("Test pattern 5 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        Query query = wisdomApp.defineQuery("query1");

        // e1<2:5> -> e2
        Pattern e1 = query.definePattern("StockStream1", "e1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);
        Pattern e2 = query.definePattern("StockStream1", "e2")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        query.from(pattern)
                .insertInto("OutputStream");

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
        LOGGER.info("Test pattern 6 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        Query query = wisdomApp.defineQuery("query1");

        // e1<2:5> -> e2
        Pattern e1 = query.definePattern("StockStream1", "e1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);
        Pattern e2 = query.definePattern("StockStream1", "e2")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        query.from(pattern)
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 20.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern7() throws InterruptedException {
        LOGGER.info("Test pattern 7 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        Query query = wisdomApp.defineQuery("query1");

        // e1<0:5> -> e2
        Pattern e1 = query.definePattern("StockStream1", "e1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(0, 5);
        Pattern e2 = query.definePattern("StockStream1", "e2")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        query.from(pattern)
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e2.symbol", "WSO2", "e2.price", 10.0, "e2.volume", 10));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 10.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern8() throws InterruptedException {
        LOGGER.info("Test pattern 8 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        Query query = wisdomApp.defineQuery("query1");

        // e1 -> e2<0:5> -> e3
        Pattern e1 = query.definePattern("StockStream1", "e1")
                .filter(Operator.GREATER_THAN_OR_EQUAL(Attribute.of("price"), 50.0)
                        .and(Operator.GREATER_THAN(Attribute.of("volume"), 100)));
        Pattern e2 = query.definePattern("StockStream1", "e2")
                .filter(Operator.LESS_THAN_OR_EQUAL(Attribute.of("price"), 40.0))
                .times(0, 5);
        Pattern e3 = query.definePattern("StockStream1", "e3")
                .filter(Operator.LESS_THAN_OR_EQUAL(Attribute.of("volume"), 70));

        Pattern pattern = Pattern.followedBy(Pattern.followedBy(e1, e2), e3);

        query.from(pattern)
                .select("e1.symbol", "e2[0].symbol", "e3.symbol")
                .insertInto("OutputStream");
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
        LOGGER.info("Test pattern 9 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        Query query = wisdomApp.defineQuery("query1");

        // e1 -> e2<:5> -> e3
        Pattern e1 = query.definePattern("StockStream1", "e1")
                .filter(Operator.GREATER_THAN_OR_EQUAL(Attribute.of("price"), 50.0)
                        .and(Operator.GREATER_THAN(Attribute.of("volume"), 100)));
        Pattern e2 = query.definePattern("StockStream1", "e2")
                .filter(Operator.LESS_THAN_OR_EQUAL(Attribute.of("price"), 40.0))
                .maxTimes(5);
        Pattern e3 = query.definePattern("StockStream1", "e3")
                .filter(Operator.LESS_THAN_OR_EQUAL(Attribute.of("volume"), 70));

        Pattern pattern = Pattern.followedBy(Pattern.followedBy(e1, e2), e3);

        query.from(pattern)
                .select("e1.symbol", "e2[0].symbol", "e3.symbol")
                .insertInto("OutputStream");
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
        LOGGER.info("Test pattern 10 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        Query query = wisdomApp.defineQuery("query1");

        // e1 -> e2<:5> -> e3
        Pattern e1 = query.definePattern("StockStream1", "e1")
                .filter(Operator.GREATER_THAN_OR_EQUAL(Attribute.of("price"), 50.0)
                        .and(Operator.GREATER_THAN(Attribute.of("volume"), 100)));
        Pattern e2 = query.definePattern("StockStream1", "e2")
                .filter(Operator.LESS_THAN_OR_EQUAL(Attribute.of("price"), 40.0))
                .maxTimes(5);
        Pattern e3 = query.definePattern("StockStream1", "e3")
                .filter(Operator.LESS_THAN_OR_EQUAL(Attribute.of("volume"), 70));

        Pattern pattern = Pattern.followedBy(Pattern.followedBy(e1, e2), e3);

        query.from(pattern)
                .select("e1.symbol", "e2[0].symbol", "e2[1].symbol", "e3.symbol")
                .insertInto("OutputStream");
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
