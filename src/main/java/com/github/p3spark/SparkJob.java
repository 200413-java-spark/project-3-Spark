package com.github.p3spark;

import com.github.p3spark.io.Database;
import com.github.p3spark.io.FileParser;
import com.github.p3spark.operation1.CountyOil;
import com.github.p3spark.operation1.SimpleTransform;
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
        Dataset<Row> csvData = new DataReader().readInFile(session);

        //in the command line, the first argument needs to be "1" for the next part of the code to work
        //additionally, it needs an integer to display a county and yearly oil production as the second argument
        if (args[0].equals("1")){
            System.out.println("This is a test!");
            try {
                new CountyOil().findTotOilByCounty(session, csvData, args[1]);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        else if (args[0].equals("2")){
    	//System.setProperty("hadoop.home.dir","C:/Program Files/hadoop");
    	Logger.getLogger("org.apache").setLevel(Level.WARN);
    	//SparkSession spark=SparkSession.builder().appName("P3 App").master("local[*]").config("spark.sql.warehouse.dir","file:///c:/tmp/").getOrCreate();
    	String n="Stark, Eric";
    	SimpleTransform instance = new SimpleTransform(session, csvData);
    	//Dataset<Row> result=instance.filterCompanyName(n);
        //Dataset<Row> result=instance.allCompanyName();
        Dataset<Row> result=instance.activeWellForCountyYearly();
    	result.show(3000);
        }

        //Writing to a database
        else if (args[0].equals("3")){
        new Database().writeToDatabase(session);
        }
        //parse out the file
        else if (args[0].equals("4")){
        new FileParser().parseFile();
        }

    	session.close();

    }
    
}