package com.github.p3spark;

import com.github.p3spark.operation1.CountyOil;
import com.github.p3spark.startup.CreateSparkSession;

import org.apache.spark.sql.SparkSession;

public class SparkJob {
    public static void main(String[] args) throws InterruptedException {
        //stuff needs to go here
        CreateSparkSession startSession = CreateSparkSession.getInstance(); //Starts SparkSession
        SparkSession session = startSession.getSession(); //pulls a reference to the session
        
        new CountyOil().findTotOilByCounty(session);
    }
    
}