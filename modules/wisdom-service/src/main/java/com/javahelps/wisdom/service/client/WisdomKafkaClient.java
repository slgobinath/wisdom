package com.javahelps.wisdom.service.client;

import com.javahelps.wisdom.service.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class WisdomKafkaClient extends WisdomClient {

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
    public Response send(String streamId, Map<String, Comparable> data) {

        try {
            final ProducerRecord<String, String> record = new ProducerRecord<>(streamId, this.wisdomAppName,
                    Utility.toJson(data));

            RecordMetadata metadata = (RecordMetadata) producer.send(record).get();

            System.out.printf("Sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n", record.key(), record
                            .value(), metadata.partition(),
                    metadata.offset());

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.flush();
        }
        return new Response(0, "");
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
