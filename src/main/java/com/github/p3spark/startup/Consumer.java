package com.github.p3spark.startup;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

import com.github.p3spark.io.Database;
import com.github.p3spark.operation1.SimpleTransform;
import com.github.p3spark.utils.ConfigProperties;

import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeUnit;

public class Consumer {
    ConfigProperties configProperties = new ConfigProperties();
    String kafkaUrl = configProperties.getKafkaurl();
    String kafkaTopic = configProperties.getKafkatopic();
    boolean flag = true;

    public Consumer() {
    }

    public void builder(SparkSession spark) {
        // added to remove messy messages
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);
        // added to remove messy messages

        // Create DataFrame representing the stream of input lines from connection to

        //creating schema to be used later to parse out in JSON format from kafka
        StructType oilSchema = new StructType()
                .add("API Well Number", "string")
                .add("County", "string")
                .add("Company Name", "string")
                .add("API Hole Number", "string")
                .add("Sidetrack Code", "string")
                .add("Completion Code", "string")
                .add("Well Type Code", "string")
                .add("Production Field", "string")
                .add("Well Status Code", "string")
                .add("Well Name", "string")
                .add("Town", "string")
                .add("Producing Formation", "string")
                .add("Months in Production", "string")
                .add("Gas Produced, Mcf", "string")
                .add("Water Produced, bbl", "string")
                .add("Oil Produced, bbl", "string")
                .add("Reporting Year", "string")
                .add("New Georeferenced Column", "string")
                .add("id", "string");

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaUrl)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "earliest")
                .load();

        StreamingQuery initDF = df
                .selectExpr("CAST(value AS STRING)")
                .writeStream()
                .option("truncate", false)
                .outputMode("append")
                .format("memory")
                .queryName("initDF")
                .trigger(Trigger.ProcessingTime(150000))
                .start();

        while (initDF.isActive()) {

            Dataset<Row> test1 = spark.sql("select * from initDF");

            Dataset<Row> json = test1.select(from_json(col("value"), oilSchema)
                    .as("data"))
                    .select("data.*");

            json = new DataReader().parseHeaders(json);

            sparkOperations(spark, json);
        }
    }

    private void sparkOperations(SparkSession spark, Dataset<Row> json) {
        if (flag) {
            flag = false;
            System.out.println("Starting init timeout");
            try {
                TimeUnit.MINUTES.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("End of init timeout");
        } else {
            long minutes = 1;

            SimpleTransform instance = new SimpleTransform(spark, json);
            Database database = new Database();

            System.out.println("Writing to database");
            Dataset<Row> result = instance.productionForCountyYearly();
            database.writeToDatabase(result, 1);
            Dataset<Row> result2 = instance.LocationYearly(true);
            database.writeToDatabase(result2, 2);
            Dataset<Row> result3 = instance.allCompany();
            database.writeToDatabase(result3, 3);
            Dataset<Row> result4 = instance.townVsWell();
            database.writeToDatabase(result4, 4);
            Dataset<Row> result5 = instance.countyVsWell();
            database.writeToDatabase(result5, 5);

            System.out.println("Timeout window = " + minutes + " minute/s");

            try {
                TimeUnit.MINUTES.sleep(minutes);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
