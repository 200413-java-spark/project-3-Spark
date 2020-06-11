package com.github.p3spark.startup;

import com.github.p3spark.utils.ConfigProperties;
import org.apache.log4j.Level;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class Consumer {
    ConfigProperties configProperties = new ConfigProperties();
    String kafkaUrl = configProperties.getKafkaurl();
    String kafkaTopic = configProperties.getKafkatopic();


    public Consumer() {
    }

    public void builder() {
//        added to remove messy messages
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);
//        added to remove messy messages

        SparkSession spark = SparkSession
                .builder()
                .appName("consumer")
                .master("local")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaUrl)
                .option("subscribe", kafkaTopic)
                .load();

        df.selectExpr("CAST(value AS STRING)");



        // Start running the query that prints the running counts to the console
        StreamingQuery query = df
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}