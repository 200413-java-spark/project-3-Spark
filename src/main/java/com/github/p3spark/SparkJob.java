package com.github.p3spark;

import com.github.p3spark.io.Database;
import com.github.p3spark.io.FileParser;
import com.github.p3spark.operation1.CountyOil;
import com.github.p3spark.operation1.SimpleTransform;
import com.github.p3spark.startup.Consumer;
import com.github.p3spark.startup.CreateSparkSession;
import com.github.p3spark.startup.DataReader;

import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkJob {
    public static void main(String[] args) {
        //Creates spark session
        CreateSparkSession startSession = CreateSparkSession.getInstance(); // Starts SparkSession
        SparkSession session = startSession.getSession(); // pulls a reference to the session
        //Generates the full CSV dataset with modified column names

        // Dataset<Row> csvData = new DataReader().readInFile(session);
        // new Consumer().builder(session);

        Dataset<Row> dummyData = session.read().option("header", "true")
        .option("sep", ",").option("inferSchema", "true").csv("dummydata.csv").toDF();

        Dataset<Row> dummyData2 = dummyData.select("County", "Oil Produced, bbl");

        new Database().writeToDatabase(session, dummyData2, "table3");
        Dataset<Row> result = new Database().readFromDatabase(session, "table3");
        result.show(20);
        
        session.close();

    }

}