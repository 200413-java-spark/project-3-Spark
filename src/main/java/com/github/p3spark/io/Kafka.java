package com.github.p3spark.io;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

/**
 * This is what I wrote down of Mehrab's Hello World in Kafka.
 * We're going to need something like this,
 */
public class Kafka {

    public static void main(String[] args) throws InterruptedException
    {
    System.out.println("*****************************Before conf*****************************************");
    SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
    JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(1));
    System.out.println("******************************After conf****************************************");

    Map<String,Object> params = new HashMap<>();
    params.put("bootstrap.servers", "localhost:9092");
    params.put("key.deserializer", StringDeserializer.class); 
    params.put("value.deserializer", StringDeserializer.class);
    params.put("group.id", "com.revature");
    params.put("auto.offset.reset", "latest");
    System.out.println("************after params**********************************************************");

    Collection<String> topics = Arrays.asList("Hello");
    System.out.println("*************after topics*********************************************************");

    JavaInputDStream<ConsumerRecord<String,String>> messageStream = KafkaUtils.createDirectStream(context, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(topics,params));
    System.out.println("***********after messagestream***********************************************************");

    JavaDStream<String> names = messageStream.map(record -> record.value());
    System.out.println("**********after names************************************************************");

    names.count().print(0);
    context.start();
    context.awaitTermination(); 
    
    }


    
}