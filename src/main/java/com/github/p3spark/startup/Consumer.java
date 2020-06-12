package com.github.p3spark.startup;

import java.util.Arrays;

import com.github.p3spark.utils.ConfigProperties;
import org.apache.log4j.Level;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.types.StructType;

public class Consumer {
        ConfigProperties configProperties = new ConfigProperties();
        String kafkaUrl = configProperties.getKafkaurl();
        String kafkaTopic = configProperties.getKafkatopic();

        public Consumer() {
        }

        public void builder(SparkSession spark) {
                // added to remove messy messages
                org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
                org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);
                org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);
                // added to remove messy messages

                /*
                 * SparkSession spark = SparkSession .builder() .appName("consumer")
                 * .master("local") .getOrCreate();
                 */

                // Create DataFrame representing the stream of input lines from connection to
                // localhost:9999

                

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
                        .add("New Georeferenced Column", "string");

                Dataset<Row> df = spark
                        .readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", kafkaUrl)
                        .option("subscribe", kafkaTopic)
                        .option("startingOffsets", "latest")
                        .load();

                // df.selectExpr("CAST(value AS STRING)");

                StreamingQuery initDF = df
                        .selectExpr("CAST(value AS STRING)")
                        .writeStream()
                        .option("truncate", false)
                        .outputMode("append")
                        .format("memory")
                        .queryName("initDF")
                        .trigger(Trigger.ProcessingTime(2000))
                        .start();

                while (initDF.isActive()) {
                        try {
                                Thread.sleep(10000);
                                Dataset<Row> test1 = spark.sql("select * from initDF");
                                //testing
                                /*Dataset<String> words = test1.as(Encoders.STRING()).flatMap(
                                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                                Encoders.STRING());
        
                                // Generate running word count
                                Dataset<Row> wordCounts = words.groupBy("value").count();
                                wordCounts.show();*/
                                /*String[] headers = test1.columns();
                                for(int i =0; i < headers.length; i++){
                                        System.out.println(headers[i]);
                                }
                                System.out.println(test1.col("value"));*/

                                test1.select(from_json(col("value"), oilSchema)
                                        .as("data"))
                                        .select("data.*")
                                        .show(10);


                        } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                        }
                }

        

        // Start running the query that prints the running counts to the console
        /*StreamingQuery query = df
                .selectExpr("CAST(value AS STRING)")
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }*/

        
    }
}