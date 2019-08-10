/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pliu.kafkastreamsapps;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public final class EmbeddedKafka {

    static String HOST = InetAddress.getLoopbackAddress().getCanonicalHostName();
    private static final int ZK_PORT = 21818;
    private static final int SERVER_PORT = 18922;

    private final KafkaServerStartable kafkaServer;
    private final EmbeddedZookeeper zookeeper;

    private KafkaProducer<String, byte[]> producer;
    private final ConcurrentHashMap<String, KafkaConsumer<String, byte[]>> consumers = new ConcurrentHashMap<>();
    private final File dir;

    public EmbeddedKafka(Properties properties) {
        zookeeper = new EmbeddedZookeeper(ZK_PORT);
        dir = new File(System.getProperty("java.io.tmpdir"), "kafka");
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper.getConnectString());
        props.put("broker.id", "1");
        props.put("host.name", "localhost");
        props.put("port", String.valueOf(SERVER_PORT));
        props.put("log.dir", dir.getAbsolutePath());
        props.put("offsets.topic.replication.factor", (short) 1);
        if (properties != null) {
            props.putAll(properties);
        }
        KafkaConfig config = new KafkaConfig(props);
        kafkaServer = new KafkaServerStartable(config);
        kafkaServer.startup();
        initProducer();
    }

    public void stop() throws IOException {
        producer.close();
        kafkaServer.shutdown();
        zookeeper.stopZookeeper();
        FileUtils.deleteDirectory(dir);
    }

    public String getBootstrapServers() {
        return HOST + ":" + SERVER_PORT;
    }

    private KafkaConsumer<String, byte[]> getConsumer(String topic) {
        if (!consumers.containsKey(topic)) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST + ":" + SERVER_PORT);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_client");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaConsumer <String, byte[]> consumer = new KafkaConsumer<>(props, new StringDeserializer(),
                    new ByteArrayDeserializer());
            consumer.subscribe(Collections.singletonList(topic));
            consumers.put(topic, consumer);
        }
        return consumers.get(topic);
    }

    private void initProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST + ":" + SERVER_PORT);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
    }

    public void produce(String topic, String k, String v) {
        produce(topic, k, v.getBytes());
    }

    public void produce(String topic, String k, byte[] v) {
        ProducerRecord<String, byte[]> rec = new ProducerRecord<>(topic, k, v);
        try {
            producer.send(rec).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public ConsumerRecords<String, byte[]> consume(String topic) {
        KafkaConsumer<String, byte[]> consumer = getConsumer(topic);
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
        consumer.commitAsync();
        return records;
    }

    public static void main(String[] args) {
        EmbeddedKafka kafka = new EmbeddedKafka(null);
        kafka.produce("test", "key1", "value1");
        kafka.produce("test", "key2", "value2");
        while (true) {
            ConsumerRecords<String, byte[]> records = kafka.consume("test");
            records.forEach(record -> System.out.println(record.key() + " " + new String(record.value())));
        }
    }
}