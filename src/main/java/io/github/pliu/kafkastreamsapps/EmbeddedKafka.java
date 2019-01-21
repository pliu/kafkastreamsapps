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

package io.github.pliu.kafkastreamsapps;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public final class EmbeddedKafka {

    static String HOST = InetAddress.getLoopbackAddress().getCanonicalHostName();
    private static final int ZK_PORT = 21818;
    private static final int SERVER_PORT = 18922;

    private final KafkaServerStartable kafkaServer;
    private final EmbeddedZookeeper zookeeper;

    private KafkaProducer<String, byte[]> producer;
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

    private void initProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", HOST + ":" + SERVER_PORT);
        props.put("acks", "1");
        producer = new KafkaProducer<>(props,
                new StringSerializer(), new ByteArraySerializer());
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
}