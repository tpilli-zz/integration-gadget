package com.github.ptn006;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaProps {
   static Properties properties = new Properties();

    public static Properties producerProperties() {

        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");

        // avro part
        properties.setProperty("key.serializer", StringSerializer .class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer .class.getName());

        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        return properties;

    }

    public static Properties consumerProperties() {

       // Properties properties = new Properties();

        // normal consumer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //different for consumer
        properties.setProperty("group.id", "Avro-consumer");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("auto.offset.reset", "earliest");

        // avro part
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader", "true");

        return properties;
    }

    public static Properties streamsProperties() {

        // Properties properties = new Properties();

        // normal consumer
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "com.github.ptn006");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return properties;
    }



}
