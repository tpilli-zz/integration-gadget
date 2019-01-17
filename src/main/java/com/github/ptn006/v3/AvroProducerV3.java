package com.github.ptn006.v3;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class AvroProducerV3 {

    public static void main(String[] args) {

        String app_id = "SQE";
        String dev_stage = "DEV";
        int i= 0;

        Properties properties = new Properties();

        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");

        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        //start db
        try{

            //Get a connection to database
            Connection myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sys",
                    "root", "root");

            //create a statement
            Statement myStmnt = myConn.createStatement();

            //Execute SQL query
            ResultSet res = myStmnt.executeQuery("select * from app_data where app_id='"+app_id+
                    "' and dev_stage='"+dev_stage+"'");

            System.out.println("-------------Inputs------------");
            System.out.println("App Id: " + app_id + ", Development stage: " + dev_stage);
            System.out.println("\n--------------Outputs Begin--------------");

            //process result set
            while (res.next()){
                String dbRecords ="Scan name: "+res.getString("scan_name") +", Result Flag: "+ res.getString("result_flg");
                i++;

                System.out.println("\n"+dbRecords);

                if(dbRecords.contains("Vera"))
                    if (dbRecords.endsWith("N"))
                        System.out.println("kick off Veracode scan");
                    else
                        System.out.println("Veracode results are available");

                else if(dbRecords.contains("Checkmarx"))
                    if(dbRecords.endsWith("N"))
                        System.out.println("Kick Checkmarx scan");
                    else
                        System.out.println("Checkmarx scan results are available");

                System.out.println("\nNumber of records returned from database:"+ i);

                KafkaProducer<String, App_V3> kafkaProducer = new KafkaProducer<String, App_V3>(properties);
                String topic = "Initiate_Service";

                String appName = res.getString("app_name");
                String scanName = res.getString("scan_name");
                int seqNum = res.getInt("seq_num");
                String resultFlg = res.getString("result_flg");


                App_V3 svc3 = App_V3.newBuilder()
                        .setAppId(app_id)
                        .setAppName(appName)
                        .setDevStage(dev_stage)
                        .setScanName(scanName)
                        .setSeqNum(seqNum)
                        .setResultFlg(resultFlg)
                        .build();

                ProducerRecord<String, App_V3> producerRecord = new ProducerRecord<String, App_V3>(topic, svc3);
                System.out.println(svc3);

                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null){
                            System.out.println("\nSuccess..!");
                            System.out.println(recordMetadata.toString());
                        } else{
                            e.printStackTrace();
                        }
                    }
                });
                kafkaProducer.flush();
                kafkaProducer.close();
            }
            System.out.println("\n--------------Outputs End--------------");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        //end db
    }

}



//partition zero and offset 4

