package com.javahelps.wisdom.dev.client;

import com.javahelps.wisdom.dev.util.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class WisdomKafkaClient extends WisdomClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomKafkaClient.class);
    private final KafkaProducer producer;
    private final String wisdomAppName;

    public WisdomKafkaClient(String wisdomAppName, String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "WisdomKafkaClient");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
        this.wisdomAppName = wisdomAppName;
    }

    @Override
    public Response send(String topic, Map<String, Object> data) {

        Response response;
        try {

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, this.wisdomAppName, Utility.toJson(data));
            RecordMetadata metadata = (RecordMetadata) producer.send(record).get();

            LOGGER.debug("Sent meta(partition={}, offset={}) record(key={} value={})",
                    record.key(), record.value(), metadata.partition(), metadata.offset());
            response = new Response(0, "Sent record to Kafka");

        } catch (InterruptedException | ExecutionException e) {
            response = new Response(-1, e.getMessage());
        } finally {
            producer.flush();
        }
        return response;
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
