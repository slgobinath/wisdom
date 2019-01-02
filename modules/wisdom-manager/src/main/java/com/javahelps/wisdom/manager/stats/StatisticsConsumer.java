/*
 * Copyright (c) 2018, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
 *
 * Gobinath licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. In addition, if you are using
 * this file in your research work, you are required to cite
 * WISDOM as mentioned at https://github.com/slgobinath/wisdom.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.javahelps.wisdom.manager.stats;

import com.javahelps.wisdom.dev.util.Utility;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StatisticsConsumer implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsConsumer.class);

    private final String groupId;
    private final String topic;
    private final String bootstrapServers;
    private final StatsListener statsListener;
    private final Lock lock = new ReentrantLock();
    private final ExecutorService executorService;
    private transient boolean active = true;
    private Consumer<String, String> consumer;

    public StatisticsConsumer(String bootstrapServers, String topic, String groupId, ExecutorService executorService, StatsListener statsListener) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.executorService = executorService;
        this.statsListener = statsListener;
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
                records.forEach(record -> this.statsListener.onStats(Utility.toMap(record.value())));
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

    public void start() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        this.consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic.
        this.consumer.subscribe(Collections.singletonList(this.topic));
        this.executorService.execute(this);
    }

    public void stop() {
        this.active = false;
        try {
            lock.lock();
            this.consumer.unsubscribe();
            this.consumer.close();
        } catch (CommitFailedException e) {
            LOGGER.error("Kafka commit failed for topic " + this.topic, e);
        } finally {
            lock.unlock();
        }
    }

    public interface StatsListener {
        void onStats(Map<String, Object> stats);
    }
}
