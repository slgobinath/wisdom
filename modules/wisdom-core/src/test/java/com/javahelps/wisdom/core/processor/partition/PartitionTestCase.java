package com.javahelps.wisdom.core.processor.partition;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.window.Window;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.javahelps.wisdom.core.TestUtil.map;

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
                .partitionBy("symbol")
                .window(Window.lengthBatch(2))
                .aggregate(Operator.SUM("price"), "price")
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
                .partitionBy("symbol", "volume")
                .window(Window.lengthBatch(2))
                .aggregate(Operator.SUM("price"), "price")
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
                .partitionBy("symbol")
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
                .filter(Operator.LESS_THAN("price", 700.0))
                .partitionBy("symbol")
                .aggregate(Operator.SUM("price"), "price")
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

        Assert.assertEquals("Incorrect number of events", 3, callback.getEventCount());
    }


    /*
        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    eventArrived = true;
                    if (count.get() == 1) {
                        Assert.assertEquals(75.5999984741211, event.getData()[1]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(151.1999969482422, event.getData()[1]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(75.5999984741211, event.getData()[1]);
                    }
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStreamOne");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 70005.6f, 100});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        executionRuntime.shutdown();

    }

    @Test
    public void testPartitionQuery2() throws InterruptedException {
        log.info("Partition test2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('PartitionTest2') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream StockStream1 (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream , symbol of StockStream1) begin @info(name = 'query1') " +
                "from cseEventStream[700>price] select symbol,sum(price) as price,volume insert into OutStockStream ;" +
                "  end ";


        SiddhiAppRuntime executionRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });
        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        Assert.assertEquals(4, count.get());
        executionRuntime.shutdown();
    }*/

}
