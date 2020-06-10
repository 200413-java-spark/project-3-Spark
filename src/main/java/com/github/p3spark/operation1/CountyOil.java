package com.github.p3spark.operation1;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class CountyOil {
    
    public CountyOil(){}

    public void findTotOilByCounty(SparkSession session, Dataset<Row> dataCSV, String arg2) throws InterruptedException {

        //select columns for counties and oil produced and year
        String[] headers = dataCSV.columns();//gets headers into a string array
        int category = 1;
        int depVariable = 15;
        int indVariable = 16;
        //selects those three columns
        Dataset<Row> data1 = dataCSV.select(headers[category], headers[depVariable], headers[indVariable]).na().drop();
        Dataset<Row> data = data1
            .withColumn(headers[depVariable], data1.col(headers[depVariable]).cast(DataTypes.DoubleType))//replaces strings with doubles
            .withColumn(headers[indVariable], data1.col(headers[indVariable]).cast(DataTypes.DoubleType))
            .cache();
        data.createOrReplaceTempView("oilByYear");
        data.printSchema();
        //counts how many counties there are
        int CCount = (int)data.select(headers[category]).distinct().count();
            List<Row> counties = new ArrayList<>();
            counties = data.select(headers[category]).distinct().takeAsList(CCount);

        List<Dataset<Row>> addList = new ArrayList<>();

        //Dataset<Row> creatingNew = data.map(func, Encoders.Strings.class);
        /*for (int i = 0; i < CCount; i++){
            //iterates through each county and shows a table of how much oil
            //was produced each year
            Dataset<Row> pickTable = session.sql("SELECT "+headers[depVariable]+
            ", "+headers[indVariable]+" FROM oilByYear WHERE "+headers[category]+
            "=\'"+counties.get(0).get(0).toString().replace("'", "\'\'")+"\'").cache();
            pickTable.createOrReplaceTempView("pickTable");
            //sums up the totals
            Dataset<Row> oilYear = session.sql("SELECT "+headers[indVariable]+
            " ,ROUND(SUM("+headers[depVariable]+
            "),2) AS Total_Oil FROM pickTable GROUP BY "+headers[indVariable]);
            oilYear.createOrReplaceTempView("ysTable");
            Dataset<Row> oilYearSorted = session.sql("SELECT * FROM ysTable ORDER BY "+headers[indVariable]);
            //shows the table
            //oilYearSorted.show();
            //System.out.println(counties.get(i).get(0).toString().replace("'", "\'\'"));
            //TimeUnit.SECONDS.sleep(5);//pauses for 5 seconds
            addList.add(oilYearSorted);
        }*/
        
            //shows the table
            //addList.get(Integer.parseInt(arg2)).show();
            //System.out.println(counties.get(Integer.parseInt(arg2)).get(0).toString().replace("'", "\'\'"));
    }
}