package com.github.p3spark.startup;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataReader {

    public DataReader(){}

    public Dataset<Row> parseHeaders(Dataset<Row> dataCSV){
        String[] headers = dataCSV.columns();//gets headers into a string array

        for (int i = 0; i < headers.length; i++){
            dataCSV = dataCSV
                .withColumnRenamed(headers[i], headers[i]
                    .replace(",", "")
                    .replace(" ", "")
                    .toLowerCase());
        }
        return dataCSV;
    }
}