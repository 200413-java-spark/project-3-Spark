package com.github.p3spark;

<<<<<<< HEAD
import com.github.p3spark.operation1.CountyOil;
import com.github.p3spark.startup.CreateSparkSession;

import org.apache.spark.sql.SparkSession;
=======


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


>>>>>>> Pejal_branch

public class SparkJob {
    public static void main(String[] args) throws InterruptedException {
        //stuff needs to go here
<<<<<<< HEAD
        CreateSparkSession startSession = CreateSparkSession.getInstance(); //Starts SparkSession
        SparkSession session = startSession.getSession(); //pulls a reference to the session
        
        new CountyOil().findTotOilByCounty(session);
=======
    	
    	System.setProperty("hadoop.home.dir","C:/Program Files/hadoop");
    	Logger.getLogger("org.apache").setLevel(Level.WARN);
    	SparkSession spark=SparkSession.builder().appName("P3 App").master("local[*]").config("spark.sql.warehouse.dir","file:///c:/tmp/").getOrCreate();
    	String n="Stark, Eric";
    	SimpleTransform instance=new SimpleTransform(spark);
    	//Dataset<Row> result=instance.filterCompanyName(n);
    	Dataset<Row> result=instance.allCompanyName();
    	result.show(3000);
    	
    
    	spark.close();
>>>>>>> Pejal_branch
    }
    
}