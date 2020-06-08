/*package com.github.p3spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
/**
 * This is something I found in "Structured Streaming Programming Guide".
 * I'm not sure how useful this is. Should this be the code that is turns the csv file into a stream that is fed into Kafka?
 * If that's the case I might be on the right track.
 * It doesn't work for me right now because of winutil.exe. Whenever I try to run it the query gives me something along the lines of 
 *      "we cannot make file in this location due to chmod 600" and when I googled the error it said the lack of winutils.exe was the issue.
 *//*
public class GarrisonSpark {

    
    public static void main(String[] args) throws StreamingQueryException
    {
        SparkSession session = SparkSession.builder().appName("spark-job").master("local[*]").getOrCreate();

        StructType userSchema = new StructType().add("APIWellNumber", "string").add("County", "string")
            .add("CompanyName", "string").add("APIHoleNumber", "string").add("SidetrackCode", "string")
            .add("CompletionCode", "string").add("WellTypeCode", "string").add("ProductionField", "string")
            .add("WellStatusCode", "string").add("WellName", "string").add("Town", "string")
            .add("ProducingFormation", "string").add("MonthsInProduction", "string").add("GasProduced", "integer")
            .add("WaterProduced", "integer").add("OilProduced", "integer").add("ReportingYear", "string")
            .add("NewGeoreferencedColumn","string");
        System.out.println("********************************************************************************");

        Dataset<Row> oilData = session.readStream()
                        .option("sep", ",").option("header",false).schema(userSchema).csv("C:\\Users\\Garrison\\project-3-Spark\\project-3-Spark\\results*.csv");
    
        System.out.println("********************************************************************************");


        Dataset<Row> oilData2 = oilData.limit(20);

        StreamingQuery query = oilData2.writeStream()
        .format("console")
        .start();
  
        System.out.println(query.isActive());
        query.awaitTermination();
        System.out.println("********************************************************************************");

      
        // API Well Number,County,Company Name,API Hole Number,Sidetrack Code,
        
        // Completion Code,Well Type Code,Production Field,Well Status Code,Well Name,Town,
        
        // Producing Formation,Months in Production,"Gas Produced, Mcf",
        
        // "Water Produced, bbl","Oil Produced, bbl",Reporting Year,New Georeferenced Column


    }
}*/