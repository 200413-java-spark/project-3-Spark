package com.github.p3spark;



import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class SparkJob {
    public static void main(String[] args){
        //stuff needs to go here
    	
    	System.setProperty("hadoop.home.dir","C:/Program Files/hadoop");
    	Logger.getLogger("org.apache").setLevel(Level.WARN);
    	SparkSession spark=SparkSession.builder().appName("P3 App").master("local[*]").config("spark.sql.warehouse.dir","file:///c:/tmp/").getOrCreate();
    	String n="Stark, Eric";
    	SimpleTransform instance=new SimpleTransform(spark);
    	//Dataset<Row> result=instance.filterCompanyName(n);
    	Dataset<Row> result=instance.allCompanyName();
    	result.show(3000);
    	
    
    	spark.close();
    }
    
}