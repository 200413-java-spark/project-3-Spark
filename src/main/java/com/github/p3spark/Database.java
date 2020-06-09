package com.github.p3spark;

import java.io.IOException;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class Database {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("db-service").getOrCreate();

        Dataset<Row> dummyData = spark.read().option("header", "true").option("sep", ",").option("inferSchema", "true")
                .csv("OaGAP2.csv");

        Dataset<Row> dummyData2 = dummyData.select("New Georeferenced Column");
        dummyData2.show(50);

        try {
            FileParser.turnIntoCSV(dummyData2, "C:\\Users\\Garrison\\project-3-Spark\\project-3-Spark\\results2.csv");
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