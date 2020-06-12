package com.github.p3spark.startup;

import org.apache.spark.sql.SparkSession;

public class CreateSparkSession {
    private SparkSession sparkSession;
    private static CreateSparkSession instance;

    //creates a singleton
    private CreateSparkSession(){}
    public static CreateSparkSession getInstance(){
        if (instance == null){
            instance = new CreateSparkSession();
            instance.createSession();
        }
        return instance;
    }

    private void createSession(){
            //creating spark session
            if (this.sparkSession == null){

            this.sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("spark")
                .getOrCreate();
            }
    }
    public SparkSession getSession(){
        return this.sparkSession;
    }
}
