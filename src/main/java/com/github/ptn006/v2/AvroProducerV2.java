package com.github.ptn006.v2;


import com.github.ptn006.SVC_V2;
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

public class AvroProducerV2 {

    public static void main(String[] args) {

        String app_id = "SQE";
        String dev_stage = "UAT";
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

            //process result set
            while (res.next()){
                System.out.println(res.getString("scan_name") +", "+
                        res.getString("result_flg"));

                KafkaProducer<String, SVC_V2> kafkaProducer = new KafkaProducer<String, SVC_V2>(properties);
                String topic = "GQS-avro";

                String appName = res.getString("app_name");
                String scanName = res.getString("scan_name");
                int seqNum = res.getInt("seq_num");
                String resultFlg = res.getString("result_flg");


                SVC_V2 svc2 = SVC_V2.newBuilder()
                        .setAppId(app_id)
                        .setAppName(appName)
                        .setDevStage(dev_stage)
                        .setScanName(scanName)
                        .setSeqNum(seqNum)
                        .setResultFlg(resultFlg)
                        .build();

                ProducerRecord<String, SVC_V2> producerRecord = new ProducerRecord<String, SVC_V2>(topic, svc2);
                System.out.println(svc2);

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
        catch (Exception e) {
            e.printStackTrace();
        }
        //end db
    }

}



//partition zero and offset 4

