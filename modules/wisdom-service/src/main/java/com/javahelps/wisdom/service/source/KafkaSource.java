package com.javahelps.wisdom.service.source;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.service.Utility;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.javahelps.wisdom.service.Constant.BOOTSTRAP;
import static com.javahelps.wisdom.service.Constant.TOPIC;
import static java.util.Map.entry;

@WisdomExtension("kafka")
public class KafkaSource extends Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);

    private WisdomApp wisdomApp;
    private final String topic;
    private final String bootstrapServers;
    private KafkaConsumerThread consumerThread;

    public KafkaSource(String bootstrapServers) {
        this(Map.ofEntries(entry(BOOTSTRAP, bootstrapServers)));
    }

    public KafkaSource(Map<String, ?> properties) {
        super(properties);
        this.bootstrapServers = (String) properties.get(BOOTSTRAP);
        this.topic = (String) properties.get(TOPIC);
        if (this.bootstrapServers == null) {
            throw new WisdomAppValidationException("Required property %s for Kafka source not found", BOOTSTRAP);
        }
    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {
        String appName = wisdomApp.getName();
        String topic = Objects.requireNonNullElse(this.topic, appName + "." + streamId);
        this.wisdomApp = wisdomApp;
        this.consumerThread = new KafkaConsumerThread(this.bootstrapServers, appName, topic, wisdomApp.getInputHandler(streamId));
    }

    @Override
    public void start() {
        this.wisdomApp.getContext().getExecutorService().execute(this.consumerThread);
    }

    @Override
    public void stop() {
        this.consumerThread.stop();
    }

    private class KafkaConsumerThread implements Runnable {

        private final String bootstrapServers;
        private final String groupId;
        private final String topic;
        private final InputHandler inputHandler;
        private final Lock lock = new ReentrantLock();
        private transient boolean active = true;
        private Consumer<String, String> consumer;

        private KafkaConsumerThread(String bootstrapServers, String groupId, String topic, InputHandler inputHandler) {
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            this.topic = topic;
            this.inputHandler = inputHandler;

            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);

            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            // Create the consumer using props.
            this.consumer = new KafkaConsumer<>(props);

            // Subscribe to the topic.
            this.consumer.subscribe(Collections.singletonList(this.topic));
        }


        @Override
        public void run() {

            while (active) {
                ConsumerRecords<String, String> records = null;
                try {
                    lock.lock();
                    records = this.consumer.poll(1000);
                } catch (CommitFailedException e) {
                    LOGGER.error("Kafka commit failed for topic " + this.topic, e);
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
                        LOGGER.error("Kafka commit failed for topic " + this.topic, e);
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
                LOGGER.error("Kafka commit failed for topic " + this.topic, e);
            } finally {
                lock.unlock();
            }
        }
    }
}
