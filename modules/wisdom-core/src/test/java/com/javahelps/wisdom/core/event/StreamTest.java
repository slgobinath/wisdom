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

package com.javahelps.wisdom.core.event;

/**
 * Created by gobinath on 6/28/17.
 */
public class StreamTest {

//    @Test
//    public void testStream1() {
//
//        WisdomApp wisdomApp = new WisdomApp();
//        Stream stockStream = wisdomApp.defineStream("StockStream");
//        wisdomApp.defineStream("OutputStream");
//
//        wisdomApp.defineQuery("query1")
//                .from("StockStream")
//                .filter(AttributeOperator("symbol").EQUAL_TO("WSO2")
//                        .and(AttributeOperator("price").ADD(100).GREATER_THAN_OR_EQUAL(160.0)))
//                .window(Window.length(3))
//                .map(AttributeOperator("price").ADD(50).AS("new_price"))
//                .select("symbol", "new_price")
//                .having(AttributeOperator("new_price").GREATER_THAN(100.0))
//                .insertInto("OutputStream");
//
//        wisdomApp.addCallback("OutputStream", EventPrinter::print);
//
//        wisdomApp.start();
//
//        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
//        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
//        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 61.0, "volume", 15));
//        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 62.0, "volume", 15));
//        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 63.0, "volume", 15));
//    }

//    @Test
//    public void testStream2() {
//
//        WisdomApp wisdomApp = new WisdomApp();
//        wisdomApp.defineStream("StockStream1");
//        wisdomApp.defineStream("StockStream2");
//        wisdomApp.defineStream("OutputStream");
//
//        Pattern e1 = Pattern.pattern("pattern", "e1", "StockStream1")
//                .filter(event -> event.get("symbol").equals("IBM"));
//        Pattern e2 = e1.followedBy("e2", "StockStream2")
////                        .filter(event -> event.get("symbol").equals("WSO2")))
//                .filter(event -> e1.event().get("price").equals(event.get("price")));
//
//        wisdomApp.defineQuery("query1")
//                .from(e2)
//                .select("e2.symbol", "e1.price")
//                .map(event -> event.rename("e2.symbol", "symbol").rename("e1.price", "price"))
//                .insertInto("OutputStream");
//
//        wisdomApp.addCallback("OutputStream", EventPrinter::print);
//
//        wisdomApp.start();
//
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
//        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
//
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 60.0, "volume", 10));
//        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
//    }

}
