package com.github.ptn006;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Random;

public class ConfigData {

    public static String appName1=null;
    public static String scanName1 = null;
    public static String resultFlg1=null;
    public static String seqNum1 = null;
    public static ArrayList<String> avroList= new ArrayList<String>();

    public static void main(String[] args) throws SQLException {

        String app_id="SQE";
        String dev_stage="UAT";

        select(app_id, dev_stage);
    }
    public static void select (String app_id, String dev_stage) throws SQLException {

        String appName=null;
        String scanName = null;
        String resultFlg=null;
        String seqNum = null;
        int i=0;
        Random rand = new Random();

       // int n = rand.nextInt(1000000) + 1;
        //int n = rand.nextInt() + 1;

        //System.out.println(n+"----------------Random number");


        PreparedStatement pStmnt = null;
        String selectQuery = "select * from app_data where app_id=? and dev_stage=?";

        try{
            //create a statement
            pStmnt = Mysql.con().prepareStatement(selectQuery);
                pStmnt.setString(1,app_id);
                pStmnt.setString(2,dev_stage);

            //Execute SQL query
            ResultSet res = pStmnt.executeQuery();

            //ArrayList<String> avroList= new ArrayList<String>();

            System.out.println("-------------Inputs------------");
            System.out.println("App Id: " + app_id + ", Development stage: " + dev_stage);
            System.out.println("\n--------------Records from Config db Begin--------------");

            //process result set
            while (res.next()) {

                 appName= res.getString("app_name");
                 scanName= res.getString("scan_name");
                 resultFlg= res.getString("result_flg");
                 seqNum= res.getString("seq_num");

                /*appName1= res.getString("app_name");
                scanName1= res.getString("scan_name");
                resultFlg1= res.getString("result_flg");
                seqNum1= res.getString("seq_num");*/


                String dbRecords = "App name: "+ appName + ", Scan name: " + scanName + ", Result Flag: " + resultFlg + ", Seq Num: "+ seqNum;
                    avroList.add(appName +"!"+ scanName +"!" + seqNum +"!"+ resultFlg +"#");

                System.out.println("kbfwevfeh: "+avroList.get(0).split("!")[0]);

                    i++;

                System.out.println("\n" + dbRecords);

                    if (dbRecords.contains("Vera"))
                        if (dbRecords.endsWith("N"))
                            System.out.println("kick off Veracode scan");
                        else
                            System.out.println("Veracode results are available");

                    else if (dbRecords.contains("Checkmarx"))
                        if (dbRecords.endsWith("N"))
                            System.out.println("Kick Checkmarx scan");
                        else
                            System.out.println("Checkmarx scan results are available");
            }
            System.out.println("\nNumber of records returned from App_Data: "+ i);
            System.out.println("\n--------------Records from Config db End--------------");

            //insertUpdate(avroList, app_id, dev_stage,myCon,pStmnt);

                                   /*for (int j=0;j<avroList.size();j++) {  //Write a loop to iterate through arraylist and then use contains
                                        if (avroList.get(j).contains("Veracode")) {
                                            if (avroList.get(j).endsWith("N"))
                                                System.out.println("kick Veracode scan");
                                            else
                                                System.out.println("Veracode scan results are available");
                                        }
                                        else if(avroList.get(j).startsWith("Checkmarx")) {
                                            if(avroList.get(j).endsWith("N"))
                                                System.out.println("Kick Checkmarx scan");
                                            else
                                                System.out.println("Checkmarx scan results are available");
                                        }
                                    }  */
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        finally{
            if(pStmnt!=null) {
                pStmnt.close();
                Mysql.con().close();
            }
        }
    }

    private static void insertUpdate(ArrayList<String> avroList, String app_id, String dev_stage,Connection myCon, PreparedStatement pStmnt) throws SQLException
    {
            String scanName=null;
            String requestId=null;
            String initiateFlg=null;
            String status="Initiated";
            String topicName="Topic3";
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateStr = dateFormat.format(new java.util.Date());
            String error_msg = null;
            int j= 0;

        try {

            for (int i = 0; i < avroList.size(); i++) {

                if (avroList.get(i).endsWith("#"))
                {
                    String s[]=avroList.get(i).split("#");
                        scanName= s[0].split("!")[1];
                        initiateFlg=s[0].split("!")[2];
                        requestId=s[0].split("!")[3];
                    System.out.println(scanName + ", " + initiateFlg + ", "+requestId);

                    if (scanName != null && initiateFlg != null && requestId != null) {
                        status = "Completed";

                        String insertQuery="insert into activity_log values(?,?,?,?,?,?,?,?,?)";
                        pStmnt = myCon.prepareStatement(insertQuery);
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

                    }
                }

                                                      /*  if (avroList.get(i).contains("#")) {
                                                            scanName = avroList.get(i).split("!!")[0];
                                                            System.out.println(scanName + ", ");
                                                        } else if (avroList.get(i).contains("ResultFlag@!")) {
                                                            resultFlg = avroList.get(i).split("@!")[1];
                                                            System.out.println(resultFlg + ", ");
                                                        } else if (avroList.get(i).contains("SeqNum@!")) {
                                                            requestId = avroList.get(i).split("@!")[1];
                                                            System.out.println(requestId + ",");
                                                        }

                                                        if (scanName != null && resultFlg != null && requestId != null) {
                                                            String queryInsert="insert into activity_log values('" + app_id +
                                                                    "','" + dev_stage +
                                                                    "','" + scanName +
                                                                    "','" + Integer.valueOf(requestId) +
                                                                    "','" + resultFlg +
                                                                    "','" + status +
                                                                    "','" + topicName +
                                                                    "','" + dateStr +
                                                                    "','" + error_msg + "');";

                                                            int updatedRowCount=myStmnt2.executeUpdate(queryInsert);

                                                        }*/
            }

            System.out.println("Number of records inserted into Activity_Log: "+ j);
        }

        catch (SQLException e)
        {
             requestId = null;
             initiateFlg = null;
             status="Failed";
             error_msg = e.toString();

             String insertQuery="insert into activity_log values(?,?,?,?,?,?,?,?,?)";
             pStmnt = myCon.prepareStatement(insertQuery);
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

    }
}
