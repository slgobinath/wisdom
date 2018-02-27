package com.javahelps.wisdom.service.source;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.service.Utility;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaSource implements Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);

    private final String bootstrapServers;
    private KafkaConsumerThread consumerThread;
    private WisdomApp wisdomApp;

    public KafkaSource(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId, InputHandler inputHandler) {
        this.wisdomApp = wisdomApp;
        this.consumerThread = new KafkaConsumerThread(this.bootstrapServers, wisdomApp.getName(), streamId, inputHandler);
    }

    @Override
    public void start() {
        this.wisdomApp.getWisdomContext().getExecutorService().execute(this.consumerThread);
    }

    @Override
    public void stop() {
        this.consumerThread.stop();
    }

    private class KafkaConsumerThread implements Runnable {

        private final String bootstrapServers;
        private final String groupId;
        private final String streamId;
        private final InputHandler inputHandler;
        private final Lock lock = new ReentrantLock();
        private transient boolean active = true;
        private Consumer<String, String> consumer;

        private KafkaConsumerThread(String bootstrapServers, String groupId, String streamId, InputHandler inputHandler) {
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            this.streamId = streamId;
            this.inputHandler = inputHandler;

            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);

            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            // Create the consumer using props.
            this.consumer = new KafkaConsumer<>(props);

            // Subscribe to the topic.
            this.consumer.subscribe(Collections.singletonList(this.streamId));
        }


        @Override
        public void run() {

            while (active) {
                ConsumerRecords<String, String> records = null;
                try {
                    lock.lock();
                    records = this.consumer.poll(1000);
                } catch (CommitFailedException e) {
                    LOGGER.error("Kafka commit failed for topic " + this.streamId, e);
                } finally {
                    lock.unlock();
                }

                if (records != null) {
                    records.forEach(record -> {
                        LOGGER.info("Received {} from Kafka partition {} with key {} and offset {}",
                                record.value(), record.partition(), record.key(), record.offset());
                        Event event = EventGenerator.generate(Utility.toMap(record.value()));
                        this.inputHandler.send(event);
                    });
                    try {
                        lock.lock();
                        if (!records.isEmpty()) {
                            this.consumer.commitAsync();
                        }
                    } catch (CommitFailedException e) {
                        LOGGER.error("Kafka commit failed for topic " + this.streamId, e);
                    } finally {
                        lock.unlock();
                    }
                }
            }
        }

        public void stop() {
            this.active = false;
            try {
                lock.lock();
                this.consumer.close();
            } catch (CommitFailedException e) {
                LOGGER.error("Kafka commit failed for topic " + this.streamId, e);
            } finally {
                lock.unlock();
            }
        }
    }
}
