package com.github.p3spark.operation1;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class SimpleTransform {

    private Dataset<Row> ds;
    private SparkSession spark;

    public SimpleTransform(SparkSession spark, Dataset<Row> data) {
        this.ds = data;
        this.spark = spark;
        ds.createOrReplaceTempView("dataInfo");
    }

    public Dataset<Row> productionForCountyYearly() {
        Dataset<Row> result = spark.sql("SELECT first(id) as id, County as county,  SUM(GasProduced_Mcf) AS totalgas,"
                + "SUM(WaterProduced_bbl) AS totalwater,SUM(OilProduced_bbl) AS totaloil, ReportingYear as reportingyear FROM dataInfo "
                + "where (GasProduced_Mcf is not null OR WaterProduced_bbl is not null OR  OilProduced_bbl is not null) and"
                + " county is not null GROUP BY county,reportingyear ORDER BY county,reportingyear");

        return result;
    }


    public Dataset<Row> LocationYearly(boolean condition) {
        Dataset<Row> first_tran = spark.sql("select DISTINCT(NewGeoreferencedColumn),County, ReportingYear,SUM(GasProduced_Mcf), "
                + "SUM(WaterProduced_bbl),SUM(OilProduced_bbl),id from dataInfo "
                + "where (GasProduced_Mcf is not null OR WaterProduced_bbl is not null OR  OilProduced_bbl is not null OR NewGeoreferencedColumn is not null) "
                + "GROUP BY NewGeoreferencedColumn,ReportingYear,County,id");
        Dataset<String> second_tran = first_tran.map((MapFunction<Row, String>) f ->
                {
                    String temp2 = new String(f.toString());
                    String temp3[] = temp2.split("\\)");
                    String a[] = temp3[0].split("\\(");
                    if(a.length >1) {
                        String b[] = a[1].split(",");
                        return b[0].trim() + "," + b[1].replace(")]", " ").trim() + "," + a[0].replace("[", "") + temp3[1].replace("]", "");
                    }
                    return null;
                }, Encoders.STRING()
        );
        second_tran.createOrReplaceTempView("dataInfo2");
        Dataset<Row> thrid_tran = spark.sql("select * from dataInfo2");
        Dataset<Row> four_tran = thrid_tran
                .withColumn("longtitude", split(col("value"), ",").getItem(0))
                .withColumn("latitude", split(col("value"), ",").getItem(1))
                .withColumn("town", split(col("value"), ",").getItem(2))
                .withColumn("state", split(col("value"), ",").getItem(3))
                .withColumn("county", split(col("value"), ",").getItem(4))
                .withColumn("year", split(col("value"), ",").getItem(5))
                .withColumn("gas", split(col("value"), ",").getItem(6))
                .withColumn("water", split(col("value"), ",").getItem(7))
                .withColumn("oil", split(col("value"), ",").getItem(8))
                .withColumn("id", split(col("value"), ",").getItem(9));
        four_tran.createOrReplaceTempView("dataInfo3");
        Dataset<Row> result = spark.sql("select longtitude,latitude,county,town,year,gas,water,oil,id from dataInfo3");
        if (!condition) {
            result.createOrReplaceTempView("dataInfo4");
            Dataset<Row> fil = spark.sql("select tbl.* from dataInfo4 tbl INNER JOIN ( Select longtitude,latitude,MIN(year)"
                    + " MinYear From dataInfo4 GROUP By longtitude,latitude)tbl1 ON tbl1.longtitude = tbl.longtitude AND tbl1.latitude=tbl.latitude Where tbl1.MinYear =tbl.Year"
                    + " ORDER BY year DESC");
            return fil;
        }

        return result;
    }


    public Dataset<Row> allCompany() {
        Dataset<Row> result = spark.sql("SELECT  DISTINCT(CompanyName) as companyname, first(id) as id, ReportingYear AS year, SUM(GasProduced_Mcf) gastotal, "
                + "SUM(WaterProduced_bbl) totalwater, SUM(OilProduced_bbl) totaloil "
                + "from dataInfo GROUP BY companyname,year ORDER BY companyname,year ASC");
        return result;
    }
}
