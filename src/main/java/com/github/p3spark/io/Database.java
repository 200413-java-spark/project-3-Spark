package com.github.p3spark.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Database {

    public Database(){}

    public void writeToDatabase(SparkSession session)
    {

        // Dataset<Row> jdbcDF = spark.read()
        //     .format("jdbc")
        //     .option("url", "jdbc:postgresql:dbserver")
        //     .option("dbtable", "schema.tablename")
        //     .option("user", "username")
        //     .option("password", "password")
        //     .load();

        String fileName = "src/resources/results.csv";
        Dataset<Row> dummyData = session.read().option("header", "true")
        .option("sep", ",").option("inferSchema", "true").csv(fileName).toDF();
       // System.out.println("***********************after dummydata init****************************************************");

        Dataset<Row> dummyData2 = dummyData.select("API Well Number", "County", "Oil Produced, bbl");
        

        // Saving data to a JDBC source
        dummyData2.write().format("jdbc")
        .option("url", "jdbc:postgresql://18.216.193.94/username")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "tablename")
        .option("user", "username")
        .option("password", "password")
        .save();

    }
}