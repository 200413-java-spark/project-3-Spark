import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
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

    public static void main(String[] args)
    {
    SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
    JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(1));
    
    Map<String,Object> params = new HashMap<>();
    params.put("bootstrap.servers", "localhost:9092");
    params.put("key.deserializer", StringDeserializer.class);
    params.put("value.deserializer", StringDeserializer.class);
    params.put("group.id", "com.revature");
    params.put("auto.offset.reset", "latest");
    
    Collection<String> messages = Arrays.asList("Hello");
    
    JavaInputDStream<ConsumerRecord<String,String>> messageStream = KafkaUtils.createDirectStream(context, LocationStrategies.PreferConsistent(),ConsumerStrategies.<String,String>Subscribe(messages,params));
    
    messageStream.map(record -> record.value());
   // System.out.println(names.count());
    
    context.start();
   // context.awaitTermination();
    
    }
    
}