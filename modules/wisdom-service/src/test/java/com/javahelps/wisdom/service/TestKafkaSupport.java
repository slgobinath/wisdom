package com.javahelps.wisdom.service;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.service.client.WisdomClient;
import com.javahelps.wisdom.service.client.WisdomKafkaClient;
import com.javahelps.wisdom.service.sink.KafkaSink;
import com.javahelps.wisdom.service.source.KafkaSource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.javahelps.wisdom.service.util.TestUtil.map;

public class TestKafkaSupport {

    static {
        ImportsManager.INSTANCE.use(KafkaSource.class);
        ImportsManager.INSTANCE.use(KafkaSink.class);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TestKafkaSupport.class);

//    @Test
    public void testKafkaSource() throws IOException, InterruptedException {

        LOGGER.info("Test Kafka source");

        final String bootstrapServer = "localhost:9092";
        final List<Event> receivedEvents = new ArrayList<>();

        // Create a WisdomApp
        WisdomApp wisdomApp = new WisdomApp("WisdomApp", "1.0.0");
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .select("symbol", "price")
                .insertInto("OutputStream");

        wisdomApp.addSource("StockStream", new KafkaSource(bootstrapServer));
        wisdomApp.addCallback("OutputStream", events -> receivedEvents.add(events[0]));

        // Create a WisdomService
        WisdomService wisdomService = new WisdomService(wisdomApp, 8080);
        wisdomService.start();

        // Let the server to start
        Thread.sleep(100);

        WisdomClient client = new WisdomKafkaClient("WisdomApp", bootstrapServer);

        client.send("StockStream", map("symbol", "IBM", "price", 50.0, "volume", 10));
        client.send("StockStream", map("symbol", "WSO2", "price", 60.0, "volume", 15));
        client.send("StockStream", map("symbol", "ORACLE", "price", 70.0, "volume", 20));

        Thread.sleep(100);

        client.close();
        wisdomService.stop();

        System.out.println(receivedEvents);

        Assert.assertEquals("First event was not received", "IBM", receivedEvents.get(0).get("symbol"));
        Assert.assertEquals("First event was not received", "WSO2", receivedEvents.get(1).get("symbol"));
        Assert.assertEquals("First event was not received", "ORACLE", receivedEvents.get(2).get("symbol"));
    }

//    @Test
    public void testKafkaSink() throws IOException, InterruptedException {

        LOGGER.info("Test Kafka sink");

        final String bootstrapServer = "localhost:9092";
        final String topic = "FilteredStocks";
        final List<Map<String, Comparable>> receivedEvents = new ArrayList<>();

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "WisdomTestConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        // Create a WisdomApp
        WisdomApp wisdomApp = new WisdomApp("WisdomApp", "1.0.0");
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .select("symbol", "price")
                .insertInto("OutputStream");

        wisdomApp.addSource("StockStream", new KafkaSource(bootstrapServer));
        wisdomApp.addSink("OutputStream", new KafkaSink(bootstrapServer, topic));

        // Create a WisdomService
        WisdomService wisdomService = new WisdomService(wisdomApp, 8080);
        wisdomService.start();

        // Let the server to start
        Thread.sleep(100);

        WisdomClient client = new WisdomKafkaClient("WisdomApp", bootstrapServer);

        client.send("StockStream", map("symbol", "IBM", "price", 50.0, "volume", 10));
        client.send("StockStream", map("symbol", "WSO2", "price", 60.0, "volume", 15));
        client.send("StockStream", map("symbol", "ORACLE", "price", 70.0, "volume", 20));

        Thread.sleep(1000L);

        // Create the consumer using props.
        ConsumerRecords<String, String> records = consumer.poll(1000);

        try {
            records.forEach(record -> {
                receivedEvents.add(Utility.toMap(record.value()));
            });
        } finally {
            consumer.commitAsync();
            consumer.close();

            client.close();
            wisdomService.stop();
        }

        Assert.assertEquals("First event was not received", "IBM", receivedEvents.get(0).get("symbol"));
        Assert.assertEquals("First event was not received", "WSO2", receivedEvents.get(1).get("symbol"));
        Assert.assertEquals("First event was not received", "ORACLE", receivedEvents.get(2).get("symbol"));
    }

}
