package com.javahelps.wisdom.service;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.service.client.WisdomAdminClient;
import com.javahelps.wisdom.service.client.WisdomClient;
import com.javahelps.wisdom.service.client.WisdomHTTPClient;
import com.javahelps.wisdom.service.sink.HTTPSink;
import com.javahelps.wisdom.service.source.HTTPSource;
import com.javahelps.wisdom.service.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.javahelps.wisdom.service.util.TestUtil.map;

public class TestWisdomService {

    static {
        ImportsManager.INSTANCE.use(HTTPSource.class);
        ImportsManager.INSTANCE.use(HTTPSink.class);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TestWisdomService.class);

    @Before
    public void createEmptyLogFile() throws IOException {
        // Create the output file
        Files.deleteIfExists(Paths.get("test_server_output.log"));
        Files.createFile(Paths.get("test_server_output.log"));
    }

    @After
    public void clean() throws IOException {
        // Create the output file
        Files.deleteIfExists(Paths.get("test_server_output.log"));
    }

    @Test
    public void testHTTPSink() throws IOException, InterruptedException {

        LOGGER.info("Test HTTP sink");

        long testServerWaitingTime = 5_000L;

        TestUtil.execTestServer(testServerWaitingTime);

        Thread.sleep(1100);

        // Create a WisdomApp
        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .select("symbol", "price")
                .insertInto("OutputStream");

        wisdomApp.addSource("StockStream", Source.create("http", map("mapping", "json")));
        wisdomApp.addSink("OutputStream", Sink.create("http", map("endpoint", "http://localhost:9999/streamReceiver")));
        wisdomApp.addCallback("OutputStream", events -> System.out.println(events[0]));

        // Create a WisdomService
        WisdomService wisdomService = new WisdomService(wisdomApp, 8080);
        wisdomService.start();

        // Let the server to start
        Thread.sleep(100);

        WisdomClient client = new WisdomHTTPClient("localhost", 8080);

        WisdomClient.Response response = client.send("StockStream", map("symbol", "IBM", "price", 50.0, "volume", 10));
        Assert.assertEquals("Failed to send input", 202, response.getStatus());

        response = client.send("StockStream", map("symbol", "WSO2", "price", 60.0, "volume", 15));
        Assert.assertEquals("Failed to send input", 202, response.getStatus());

        response = client.send("StockStream", map("symbol", "ORACLE", "price", 70.0, "volume", 20));
        Assert.assertEquals("Failed to send input", 202, response.getStatus());

        Thread.sleep(testServerWaitingTime);

        wisdomService.stop();
        client.close();

        List<String> lines = Files.readAllLines(Paths.get("test_server_output.log"));
        lines.removeIf(line -> line.startsWith("INFO"));

        Assert.assertTrue(lines.get(0).contains("IBM"));
        Assert.assertTrue(lines.get(1).contains("WSO2"));
        Assert.assertTrue(lines.get(2).contains("ORACLE"));
        Assert.assertEquals("Invalid number of response", 3, lines.size());
    }

    @Test
    public void testShutdown() throws IOException, InterruptedException {

        LOGGER.info("Test the shutdown REST API");

        // Create a WisdomApp
        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .select("symbol", "price")
                .insertInto("OutputStream");

        wisdomApp.addSource("StockStream", Source.create("http", map("mapping", "json")));
        wisdomApp.addCallback("OutputStream", events -> System.out.println(events[0]));

        // Create a WisdomService
        WisdomService wisdomService = new WisdomService(wisdomApp, 8081);

        wisdomService.start();

        // Let the server to start
        Thread.sleep(100);

        Assert.assertTrue("WisdomService is not running", wisdomService.isRunning());

        // Stop the server
        WisdomAdminClient client = new WisdomAdminClient("localhost", 8081);
        client.stop();
        client.close();

        // Let the server to shutdown
        Thread.sleep(100);

        Assert.assertFalse("WisdomService has not been shutdown", wisdomService.isRunning());

    }


    @Test
    public void testLoadQueryFile() throws IOException, InterruptedException, URISyntaxException {

        LOGGER.info("Test load Wisdom query file");

        long testServerWaitingTime = 5_000L;

        TestUtil.execTestServer(testServerWaitingTime);

        Thread.sleep(1100);

        String path = Paths.get(ClassLoader.getSystemClassLoader().getResource("http_test.wisdomql").toURI()).toAbsolutePath().toString();
        WisdomService.main(new String[]{"--port", "8081", path});

        Thread.sleep(100);

        WisdomAdminClient client = new WisdomAdminClient("localhost", 8081);
        WisdomClient.Response response = client.send("StockStream", map("symbol", "IBM", "price", 50.0, "volume", 10));
        Assert.assertEquals("Failed to send input", 202, response.getStatus());

        response = client.send("StockStream", map("symbol", "WSO2", "price", 60.0, "volume", 15));
        Assert.assertEquals("Failed to send input", 202, response.getStatus());

        response = client.send("StockStream", map("symbol", "ORACLE", "price", 70.0, "volume", 20));
        Assert.assertEquals("Failed to send input", 202, response.getStatus());

        Thread.sleep(testServerWaitingTime);

        client.stop();

        List<String> lines = Files.readAllLines(Paths.get("test_server_output.log"));
        lines.removeIf(line -> line.startsWith("INFO"));

        Assert.assertTrue(lines.get(0).contains("WSO2"));
        Assert.assertEquals("Invalid number of response", 1, lines.size());
    }

}
