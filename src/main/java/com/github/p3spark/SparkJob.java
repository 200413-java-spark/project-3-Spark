package com.github.p3spark;

import com.github.p3spark.operation1.CountyOil;
import com.github.p3spark.operation1.SimpleTransform;
import com.github.p3spark.startup.CreateSparkSession;

import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkJob {
    public static void main(String[] args) {
        // stuff needs to go here

        CreateSparkSession startSession = CreateSparkSession.getInstance(); // Starts SparkSession
        SparkSession session = startSession.getSession(); // pulls a reference to the session

        try {
            new CountyOil().findTotOilByCounty(session);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
    	//System.setProperty("hadoop.home.dir","C:/Program Files/hadoop");
    	Logger.getLogger("org.apache").setLevel(Level.WARN);
    	//SparkSession spark=SparkSession.builder().appName("P3 App").master("local[*]").config("spark.sql.warehouse.dir","file:///c:/tmp/").getOrCreate();
    	String n="Stark, Eric";
    	SimpleTransform instance=new SimpleTransform(session);
    	//Dataset<Row> result=instance.filterCompanyName(n);
    	Dataset<Row> result=instance.allCompanyName();
    	result.show(3000);
    	
    	session.close();

    }
    
}