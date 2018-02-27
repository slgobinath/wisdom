package com.javahelps.wisdom.core.state;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.stream.Stream;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.util.EventPrinter;
import com.javahelps.wisdom.core.window.Window;
import org.junit.Test;

/**
 * Test store and restore Wisdom state.
 */
public class WisdomStateTest {

    @Test
    public void testStream1() {

        WisdomApp wisdomApp = new WisdomApp();
        Stream stockStream = wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .filter(Operator.EQUALS("symbol", "WSO2"))
                .window(Window.length(3))
                .select("symbol", "price")
//                .having(AttributeOperator.attribute("price").GREATER_THAN(100.0))
                .insertInto("OutputStream");

        wisdomApp.addCallback("OutputStream", EventPrinter::print);

        wisdomApp.start();

        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 61.0, "volume", 15));
        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 62.0, "volume", 15));
        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 63.0, "volume", 15));
    }

}
