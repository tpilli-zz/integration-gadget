package com.github.ptn006.v1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcDemo {
    public static void main(String[] args) {
        String app_id="SQE";
        String dev_stage="DEV";
        int i=0;

        try{

            //Get a connection to database
            Connection myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sys",
                    "root", "root");

            //create a statement
            Statement myStmnt = myConn.createStatement();

            //Execute SQL query
            ResultSet res = myStmnt.executeQuery("select * from app_data where app_id='"+app_id+
                    "' and dev_stage='"+dev_stage+"'");

            //ArrayList<String> avroList= new ArrayList<String>();
            System.out.println("-------------Inputs------------");
            System.out.println("App Id: " + app_id + ", Development stage: " + dev_stage);
            System.out.println("\n--------------Outputs Begin--------------");
            //process result set
            while (res.next()){
                //avroList.add(res.getString("scan_name") +","+ res.getString("result_flg"));
                //System.out.println(avroList.get(i));
                //i++;
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
            }

            System.out.println("\nNumber of records returned from database:"+ i);
            System.out.println("\n--------------Outputs End--------------");

           /* for (int j=0;j<avroList.size();j++) {  //Write a loop to iterate through arraylist and then use contains
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
            }
*/

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
