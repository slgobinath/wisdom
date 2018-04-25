package com.javahelps.wisdom.service.sink;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.dev.util.Utility;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.javahelps.wisdom.dev.util.Constants.*;
import static java.util.Map.entry;

@WisdomExtension("kafka")
public class KafkaSink extends Sink {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

    private String streamId;
    private final String topic;
    private final boolean batch;
    private final String bootstrapServers;
    private Producer<String, String> producer;

    public KafkaSink(String bootstrapServers, String topic) {
        this(bootstrapServers, topic, false);
    }

    public KafkaSink(String bootstrapServers, String topic, boolean batch) {
        this(Map.ofEntries(entry(BOOTSTRAP, bootstrapServers), entry(TOPIC, topic), entry(BATCH, batch)));
    }

    public KafkaSink(Map<String, Comparable> properties) {
        super(properties);
        this.bootstrapServers = (String) properties.get(BOOTSTRAP);
        this.topic = (String) properties.get(TOPIC);
        this.batch = (boolean) properties.getOrDefault(BATCH, false);

        if (this.bootstrapServers == null) {
            throw new WisdomAppValidationException("Required property %s for Kafka sink not found", BOOTSTRAP);
        }
        if (this.topic == null) {
            throw new WisdomAppValidationException("Required property %s for Kafka sink not found", TOPIC);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {
        this.streamId = streamId;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, streamId + "_Producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
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
