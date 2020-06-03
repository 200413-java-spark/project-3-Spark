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
 */
public class GarrisonSpark {

    public static void main(String[] args) throws StreamingQueryException
    {
        System.out.println("********************************************************************************");
        SparkSession session = SparkSession.builder().appName("spark-job").master("local[*]").getOrCreate();

        StructType userSchema = new StructType().add("API Well Number", "string").add("County", "string")
            .add("Company Name", "string").add("API Hole Number", "string").add("Sidetrack Code", "string")
            .add("Completion Code", "string").add("Well Type Code", "string").add("Production Field", "string")
            .add("Well Status Code", "string").add("Well Name", "string").add("Town", "string")
            .add("Producing Formation", "string").add("Months in Production", "string").add("Gas Produced, Mcf", "integer")
            .add("Water Produced, bbl", "integer").add("Oil Produced, bbl", "integer").add("Reporting Year", "string")
            .add("New Georeferenced Column","string");

        Dataset<Row> oilData = session.readStream()
                        .option("sep", ",").schema(userSchema).csv("C:\\Users\\Garrison\\project-3-Spark\\project-3-Spark\\dummydata.csv");
    
        
        StreamingQuery query = oilData.writeStream()
        .format("console")
        .start();
  
      query.awaitTermination();
      
        // API Well Number,County,Company Name,API Hole Number,Sidetrack Code,
        
        // Completion Code,Well Type Code,Production Field,Well Status Code,Well Name,Town,
        
        // Producing Formation,Months in Production,"Gas Produced, Mcf",
        
        // "Water Produced, bbl","Oil Produced, bbl",Reporting Year,New Georeferenced Column


    }
}