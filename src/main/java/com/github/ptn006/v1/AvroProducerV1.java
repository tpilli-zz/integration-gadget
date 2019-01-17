package com.github.ptn006.v1;


import com.github.ptn006.mySVCTest;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AvroProducerV1 {

    public static void main(String[] args) {

        Properties properties = new Properties();

        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");

        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        KafkaProducer<String, mySVCTest> kafkaProducer = new KafkaProducer<String, mySVCTest>(properties);
        String topic = "GQS-avro";

        mySVCTest mySVC = mySVCTest.newBuilder()
                .setAppId("SQE")
                .setAppName("iSEAL")
                .setDevStage("SYS")
                .setScanName("Veracode")
                .setSeqNum(9)
                .setResultFlg(true)
                .build();

        ProducerRecord<String, mySVCTest> producerRecord = new ProducerRecord<String, mySVCTest>(topic, mySVC);
        System.out.println(mySVC);

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    System.out.println("Success..!");
                    System.out.println(recordMetadata.toString());
                } else{
                    e.printStackTrace();
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}



//partition zero and offset 4

