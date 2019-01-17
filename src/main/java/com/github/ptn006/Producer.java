package com.github.ptn006;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Random;

public class Producer {

    public static void topicsData () throws SQLException {

        String app_id = InputData.app_id;
        String dev_stage = InputData.dev_stage;
        String appName = null;
        String scanName=null;
        String seqNum=null;
        String resultFlg=null;
        int j= 0;
       //Logger logger = null;

        Random rand = new Random();
       int k = rand.nextInt(10000) + 1;
       // int k=7777;
        System.out.println(k+" ----------------Random value");

        ConfigData.select(app_id, dev_stage);
        try{
            String topic2 = "Initiate_scans";
            System.out.println("-------------Inputs------------");
            System.out.println("App Id: " + app_id + ", Development stage: " + dev_stage);
            System.out.println("\n--------------Outputs Begin--------------");

            //process result set
            System.out.println(ConfigData.avroList.get(0));
            for (int i = 0; i < ConfigData.avroList.size(); i++) {
                j++;
                if (ConfigData.avroList.get(i).endsWith("#")) {
                    String s[] = ConfigData.avroList.get(i).split("#");
                    appName = s[0].split("!")[0];
                    scanName = s[0].split("!")[1];
                    seqNum = s[0].split("!")[2];
                    resultFlg = s[0].split("!")[3];
                    System.out.println("inputs from ConfigData avroList: " + appName + scanName + seqNum + resultFlg);
                }
                System.out.println("before");
                insertUpdate(app_id,dev_stage,scanName,Integer.toString(k),resultFlg,"Initiated",topic2,null);
                System.out.println("after");

                KafkaProducer<String, Initiate_Scans> kafkaProducer2 = new KafkaProducer<String, Initiate_Scans>(KafkaProps.producerProperties());
                Initiate_Scans scans = Initiate_Scans.newBuilder()
                        .setAppId(app_id)
                        .setAppName(appName)
                        .setDevStage(dev_stage)
                        .setScanName(scanName)
                        .setSeqNum(Integer.parseInt(seqNum))
                        .setResultFlg(resultFlg)
                        .setRequestId(k)
                        .setScanNumber(j)
                        .build();

                ProducerRecord<String, Initiate_Scans> producerRecord2 = new ProducerRecord<String, Initiate_Scans>(topic2, scans);
                System.out.println("Inside loop - Initiating scan: " + scans);
                try {
                    kafkaProducer2.send(producerRecord2, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            try {
                                String scanName = scans.getScanName();
                                String k = Integer.toString(scans.getRequestId());
                                String resultFlg = scans.getResultFlg();

                                if (e == null) {
                                    System.out.println("\nSuccess..!");
                                    System.out.println(recordMetadata.toString());
                                    System.out.println(scans.getAppId());
                                    insertUpdate(app_id, dev_stage, scanName, k, resultFlg, "Completed", topic2, null);
                                } else {
                                    System.out.println("Else Exception-----Topic2");
                                    insertUpdate(app_id, dev_stage, scanName, k, resultFlg, "Failed", topic2, e.toString());
                                    e.printStackTrace();
                                }
                            } catch (SQLException e2) {
                                System.out.println("Catch Exception-----Topic2");
                                e2.printStackTrace();
                            }
                        }
                    });
                }
                catch (Exception e){
                    System.out.println("Exception in Topic2::::::: ");
                    String l = Integer.toString(scans.getRequestId());
                    insertUpdate(app_id, dev_stage, scanName, l, resultFlg, "Failed", topic2, e.toString());
                    e.printStackTrace();
                }
                finally {
                    kafkaProducer2.flush();
                    kafkaProducer2.close();
                }
            }

            String topic1 = "Initiate_services";
            KafkaProducer<String, Service> kafkaProducer1 = new KafkaProducer<String, Service>(KafkaProps.producerProperties());
            insertUpdate(app_id,dev_stage,"",Integer.toString(k),"","Initiated",topic1,"");
            Service identifyTests = Service.newBuilder()
                    .setAppId(app_id)
                    .setDevStage(dev_stage)
                    .setRequestId(Integer.toString(k))
                    .setTotalScans(Integer.toString(j))
                    .build();
            ProducerRecord<String, Service> producerRecord1 = new ProducerRecord<String, Service>(topic1, identifyTests);
            System.out.println("Outisde loop - writing back to Application: "+identifyTests);
            try {
                kafkaProducer1.send(producerRecord1, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                       // String k = Integer.toString(identifyTests.getRequestId());
                        String k = identifyTests.getRequestId();
                        try {
                            if (e == null) {
                                System.out.println("\nSuccess..!");
                                System.out.println(recordMetadata.toString());
                                insertUpdate(app_id, dev_stage, "", k, "", "Completed", topic1,null);
                            } else {
                                System.out.println("Else Exception----- Topic 1");
                                e.printStackTrace();
                            }
                        } catch (SQLException e1) {
                            System.out.println("Catch----- Topic 1");
                            e1.printStackTrace();
                        }
                    }
                });
            }
            catch(Exception e){
                System.out.println("Exception in Topic1:::::::::: ");
               // String l = Integer.toString(identifyTests.getRequestId());
                String l = identifyTests.getRequestId();
                insertUpdate(app_id, dev_stage, "", l, "", "Failed", topic1, e.toString());
                e.printStackTrace();
            }
            finally {
                kafkaProducer1.flush();
                kafkaProducer1.close();
                System.out.println("\nNumber of records returned from database:"+ j);
                System.out.println("\n--------------Outputs End--------------");
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally{
            Mysql.con().close();
        }
    }

    public static void insertUpdate(String app_id, String dev_stage, String scanName, String requestId,
                                     String initiateFlg, String status, String topicName, String error_msg) throws SQLException
    {
        PreparedStatement pStmnt = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = dateFormat.format(new java.util.Date());
        int j= 0;

        try {
            System.out.println("insert try----");
            String insertQuery="insert into activity_log values(?,?,?,?,?,?,?,?,?)";
            pStmnt = Mysql.con().prepareStatement(insertQuery);
                pStmnt.setString (1,app_id);
                pStmnt.setString(2,dev_stage);
                pStmnt.setString(3,scanName);
                pStmnt.setString(4,requestId);
                pStmnt.setString(5,initiateFlg);
                pStmnt.setString(6,status);
                pStmnt.setString(7,topicName);
                pStmnt.setString(8,dateStr);
                pStmnt.setString(9,error_msg);

            int insertRecords = pStmnt.executeUpdate();
            j++;
            System.out.println("Number of records inserted into Activity_Log: "+ j);
        }
        catch (SQLException e)
        {
            status="Failed";
            error_msg = e.toString();
            System.out.println("insert catch-------");
            String insertQuery="insert into activity_log values(?,?,?,?,?,?,?,?,?)";
            pStmnt = Mysql.con().prepareStatement(insertQuery);
                pStmnt.setString (1,app_id);
                pStmnt.setString(2,dev_stage);
                pStmnt.setString(3,scanName);
                pStmnt.setString(4,requestId);
                pStmnt.setString(5,initiateFlg);
                pStmnt.setString(6,status);
                pStmnt.setString(7,topicName);
                pStmnt.setString(8,dateStr);
                pStmnt.setString(9,error_msg);

            int insertRecords = pStmnt.executeUpdate();
            j++;

            System.out.println("Number of records inserted into Activity_Log: "+ j);
            e.printStackTrace();
        }
        finally{
            if(pStmnt!=null) {
                pStmnt.close();
                Mysql.con().close();
            }
        }

    }
}

//partition zero and offset 4

