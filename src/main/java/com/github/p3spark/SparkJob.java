package com.github.p3spark;

import com.github.p3spark.startup.Consumer;
import com.github.p3spark.startup.CreateSparkSession;

import org.apache.spark.sql.SparkSession;

public class SparkJob {
    public static void main(String[] args) {
        //Creates spark session
        CreateSparkSession startSession = CreateSparkSession.getInstance(); // Starts SparkSession
        SparkSession session = startSession.getSession(); // pulls a reference to the session

        new Consumer().builder(session);
        session.close();

    }

}