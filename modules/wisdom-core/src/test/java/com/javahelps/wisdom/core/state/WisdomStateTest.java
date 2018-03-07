package com.javahelps.wisdom.core.state;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.Operator;
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
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .window(Window.lengthBatch(3))
                .aggregate(Operator.SUM("volume", "sum"))
                .select("symbol", "sum")
                .insertInto("OutputStream");

        wisdomApp.addCallback("OutputStream", EventPrinter::print);

        wisdomApp.start();

        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 1));
        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 2));
        wisdomApp.clear();
        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "ORACLE", "price", 61.0, "volume", 3));
        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "GOOGLE", "price", 62.0, "volume", 4));
        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "AMAZON", "price", 63.0, "volume", 5));
    }

}
