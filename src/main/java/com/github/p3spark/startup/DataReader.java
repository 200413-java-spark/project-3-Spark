package com.github.p3spark.startup;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataReader {

    public DataReader(){}

    public Dataset<Row> readInFile(SparkSession session){
        //reads in CSV
        String fileName = "src/main/resources/OaGAP2.csv";
        Dataset<Row> dataCSV = session
            .read()
            .format("csv")
            .option("header", "true")
            .load(fileName);
        //select columns for counties and oil produced and year
        
        String[] headers = dataCSV.columns();//gets headers into a string array

        for (int i = 0; i < headers.length; i++){
            dataCSV = dataCSV
                .withColumnRenamed(headers[i], headers[i]
                    .replace(",", "_")
                    .replace(" ", ""));
        }
        return dataCSV;
    }
}