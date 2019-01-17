package com.github.ptn006;


import com.github.ptn006.v4.App_V4;
import com.github.ptn006.v5.App_V5;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

public class Consumer {

    public static void main(String[] args) {
        try {
        String topic1 = "Initiate_services"; //remains same
        KafkaConsumer<String, Service> kafkaConsumer1 = new KafkaConsumer<String, Service>(KafkaProps.consumerProperties());

        kafkaConsumer1.subscribe(Collections.singleton(topic1)); //means subscribe to one topic
        System.out.println("Waiting for data..");
            ConsumerRecords<String, Service> consumerRecords1 = kafkaConsumer1.poll(500);
            for(ConsumerRecord<String, Service> record : consumerRecords1){
                Service service = record.value();
                System.out.println(service);
                System.out.println("Total number of scans topic1: "+service.getTotalScans());
            }
            kafkaConsumer1.commitSync();
            kafkaConsumer1.close();

            String topic2 = "Initiate_scans"; //remains same
            KafkaConsumer<String, Initiate_Scans> kafkaConsumer2 = new KafkaConsumer<String, Initiate_Scans>(KafkaProps.consumerProperties());

            kafkaConsumer2.subscribe(Collections.singleton(topic2)); //means subscribe to one topic
            System.out.println("Waiting for data..");
           // while (true) {
                ConsumerRecords<String, Initiate_Scans> consumerRecords2 = kafkaConsumer2.poll(500);
                for (ConsumerRecord<String, Initiate_Scans> record : consumerRecords2) {
                    Initiate_Scans scans = record.value();
                    System.out.println(scans);
                    System.out.println("Total number of scans topic 2: " + scans.getRequestId());
                }
                kafkaConsumer2.commitSync();
           // }
            kafkaConsumer2.close(); //not reachable and not required bcz of while loop is true
        }
        catch(Exception e){
            //Producer.insertUpdate(app_id, dev_stage, scanName, l, resultFlg, "Failed", topic2, e.toString());
            e.printStackTrace();
        }
    }
}

// If consumer is restarted, we will see 'Waiting for data' as old records will be deleted.
// Again if we run Producer then we will see new reocrds.

