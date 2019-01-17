package com.github.ptn006.v4;


import com.github.ptn006.KafkaProps;
import com.github.ptn006.v5.App_V5;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

public class AvroConsumerV4 {

    public static void main(String[] args) {
        try {
        String topic1 = "Svc_Avro_v6"; //remains same
        KafkaConsumer<String, App_V4> kafkaConsumer1 = new KafkaConsumer<String, App_V4>(KafkaProps.consumerProperties());

        kafkaConsumer1.subscribe(Collections.singleton(topic1)); //means subscribe to one topic
        System.out.println("Waiting for data..");
            ConsumerRecords<String, App_V4> consumerRecords1 = kafkaConsumer1.poll(500);
            for(ConsumerRecord<String, App_V4> record : consumerRecords1){
                App_V4 svc1 = record.value();
                System.out.println(svc1);
                System.out.println("Total number of scans topic1: "+svc1.getTotalScans());
            }
            kafkaConsumer1.commitSync();
            kafkaConsumer1.close();

            String topic2 = "Svc_Avro_v7"; //remains same
            KafkaConsumer<String, App_V5> kafkaConsumer2 = new KafkaConsumer<String, App_V5>(KafkaProps.consumerProperties());

            kafkaConsumer2.subscribe(Collections.singleton(topic2)); //means subscribe to one topic
            System.out.println("Waiting for data..");
           // while (true) {
                ConsumerRecords<String, App_V5> consumerRecords2 = kafkaConsumer2.poll(500);
                for (ConsumerRecord<String, App_V5> record : consumerRecords2) {
                    App_V5 svc2 = record.value();
                    System.out.println(svc2);
                    System.out.println("Total number of scans topic 2: " + svc2.getRequestId());
                }
                kafkaConsumer2.commitSync();
           // }
            kafkaConsumer2.close(); //not reachable and not required bcz of while loop is true
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}

// If consumer is restarted, we will see 'Waiting for data' as old records will be deleted.
// Again if we run Producer then we will see new reocrds.

