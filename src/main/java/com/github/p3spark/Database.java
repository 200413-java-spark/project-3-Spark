package com.github.p3spark;

import java.io.IOException;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class Database {

    // public Dataset<Row> productionForCountyYearly()
    // {
    //     // Dataset<Row> result=spark.sql("SELECT County, SUM(GasProduced_Mcf) AS Total_GAS_Mcf,"
    //     // 		+ "SUM(WaterProduced_bbl) AS Total_Water_BBL,SUM(OilProduced_bbl) AS Total_Oil_BBL, ReportingYear FROM dataInfo "
    //     // 		+ "where (GasProduced_Mcf is not null OR WaterProduced_bbl is not null OR  OilProduced_bbl is not null) AND County is not null "
    //     // 		+ "GROUP BY County,ReportingYear ORDER BY County,ReportingYear");
        
    //     Dataset<Row> result = ds.select("County").groupBy("County").sum("Gas Produced, Mcf");

    //     return result;
    // }
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("db-service").getOrCreate();

        Dataset<Row> dummyData = spark.read()
            .option("header", "true")
            .option("sep", ",")
            .option("inferSchema", "true")
            .option("multiline","true")
            .option("maxColumns",18)
            .csv("OaGAP.csv");

        
                
            //    dummyData.show(30);
            
        
         //Dataset<Row> dummyData2 = dummyData.select("New Georeferenced Column");


        try {
            FileParser.turnIntoCSV(dummyData, "C:\\Users\\Garrison\\project-3-Spark\\project-3-Spark\\results2.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Saving data to a JDBC source
        // dummyData2.write().format("jdbc")
        // .option("url", "jdbc:postgresql://18.216.193.94/username")
        // .option("driver", "org.postgresql.Driver")
        // .option("dbtable", "tablename")
        // .option("user", "username")
        // .option("password", "password")
        // .save();

//longitude, latitude, county,town, first reported year
    }
}