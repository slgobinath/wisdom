package com.javahelps.wisdom.service.sink;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.service.Utility;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaSink implements Sink {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

    private final String bootstrapServers;
    private final boolean batch;
    private final Producer<String, String> producer;
    private final String topic;
    private String streamId;


    public KafkaSink(String bootstrapServers, String topic) {
        this(bootstrapServers, topic, false);
    }

    public KafkaSink(String bootstrapServers, String topic, boolean batch) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.batch = batch;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "WisdomKafkaEventProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void start() {

    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {
        this.streamId = streamId;
    }

    @Override
    public void publish(List<Event> events) {

        try {
            if (this.batch) {
                this.publish(Utility.toJson(events));
            } else {
                for (Event event : events) {
                    this.publish(Utility.toJson(event));
                }
            }
        } catch (WisdomServiceException ex) {
            LOGGER.error("Failed to send Kafka event", ex);
        }
    }

    @Override
    public void stop() {
        this.producer.close();
    }

    private void publish(String json) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(this.topic, this.streamId, json);
            producer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new WisdomServiceException(String.format("Error in sending event %s to Kafka topic %s", json, this.topic));
        } finally {
            producer.flush();
        }
    }
}
