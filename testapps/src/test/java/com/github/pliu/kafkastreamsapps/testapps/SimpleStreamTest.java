package com.github.pliu.kafkastreamsapps.testapps;

import com.github.pliu.kafkastreamsapps.EmbeddedKafka;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;


class SimpleStreamTest {

    private EmbeddedKafka kafkaServer;

    @BeforeEach
    void setup() {
        kafkaServer = new EmbeddedKafka(null);
        kafkaServer.produce("test", "lol", "test");
        kafkaServer.produce("test", "lol", "test2");
        kafkaServer.produce("test", "lol2", "test");
    }

    @AfterEach
    void teardown() throws IOException {
        kafkaServer.stop();
    }

    @Test
    void testSimpleStream() throws Exception {
        KafkaStreams streams = SimpleStream.getStream(kafkaServer.getBootstrapServers());
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Thread.sleep(5000);
        streams.close();
    }
}