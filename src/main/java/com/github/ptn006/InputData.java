package com.github.ptn006;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class InputData {
    public static String app_id = "SQE";
    public static String dev_stage = "UAT";

    public static void main(String[] args) {

        String topic = "Initiate_services";

        KafkaProducer<String, Service> kafkaProducer = new KafkaProducer<String, Service>(KafkaProps.producerProperties());
        //KStreamBuilder builder = new KStreamBuilder();

        Service service = Service.newBuilder()
                .setAppId(app_id)
                .setDevStage(dev_stage)
                .build();

        ProducerRecord<String, Service> producerRecord = new ProducerRecord<String, Service>(topic, service);
        System.out.println("Outisde loop - writing to Kafka topic: " + service);
        try {
            Producer.topicsData();

            ConfigData.select(app_id, dev_stage);

            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("\nSuccess..!");
                        System.out.println(recordMetadata.toString());
                    } else {
                        System.out.println("Else Exception----- Topic 1");
                        e.printStackTrace();
                    }
                }
            });

        } catch (Exception e) {
            System.out.println("Exception in Topic:::::::::: ");
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }
}
