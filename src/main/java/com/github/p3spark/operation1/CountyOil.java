package com.github.p3spark.operation1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class CountyOil {
    
    public CountyOil(){}

    public void findTotOilByCounty(SparkSession session) throws InterruptedException {
        //reads in CSV
        String fileName = "src/resources/OaGAP.csv";
        Dataset<Row> dataCSV = session
            .read()
            .format("csv")
            .option("header", "true")
            .load(fileName);
        //select columns for counties and oil produced and year
        String[] headers = dataCSV.columns();//gets headers into a string array
        int cate = 1;
        int depv = 15;
        int indv = 16;
        String depvName = "OilProduced_bbl";
        String indvName = "YearReported";
        //selects those three columns
        Dataset<Row> data1 = dataCSV.select(headers[cate], headers[depv], headers[indv]).na().drop();
        Dataset<Row> data = data1
            .withColumn(headers[depv], data1.col(headers[depv]).cast(DataTypes.DoubleType))//replaces strings with doubles
            .withColumn(headers[indv], data1.col(headers[indv]).cast(DataTypes.DoubleType))
            .withColumnRenamed(headers[depv], depvName)//changes column names with names with no spaces and commas
            .withColumnRenamed(headers[indv], indvName)
            .cache();
        data.createOrReplaceTempView("oilByYear");
        headers[depv] = depvName;
        headers[indv] = indvName;
        //counts how many counties there are
        int CCount = (int)data.select(headers[cate]).distinct().count();
            List<Row> counties = new ArrayList<>();
            counties = data.select(headers[cate]).distinct().takeAsList(CCount);

        List<Dataset<Row>> addList = new ArrayList<>();

        for (int i = 0; i < CCount; i++){
            //iterates through each county and shows a table of how much oil
            //was produced each year
            Dataset<Row> pickTable = session.sql("SELECT "+headers[depv]+
            ", "+headers[indv]+" FROM oilByYear WHERE "+headers[cate]+
            "=\'"+counties.get(i).get(0).toString().replace("'", "\'\'")+"\'").cache();
            pickTable.createOrReplaceTempView("pickTable");
            //sums up the totals
            Dataset<Row> oilYear = session.sql("SELECT "+headers[indv]+
            " ,ROUND(SUM("+headers[depv]+
            "),2) AS Total_Oil FROM pickTable GROUP BY "+headers[indv]);
            oilYear.createOrReplaceTempView("ysTable");
            Dataset<Row> oilYearSorted = session.sql("SELECT * FROM ysTable ORDER BY "+headers[indv]);
            //shows the table
            //oilYearSorted.show();
            //System.out.println(counties.get(i).get(0).toString().replace("'", "\'\'"));
            //TimeUnit.SECONDS.sleep(5);//pauses for 5 seconds
            addList.add(oilYearSorted);
        }

            
    }
}