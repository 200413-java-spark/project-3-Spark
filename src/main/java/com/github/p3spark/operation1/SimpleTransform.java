package com.github.p3spark.operation1;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SimpleTransform {
    private Dataset<Row> ds;
    private SparkSession spark;

    public SimpleTransform(SparkSession spark, Dataset<Row> data) {
        this.ds = data;
        this.spark = spark;
        ds.createOrReplaceTempView("dataInfo");

        Dataset<Row> result = spark.sql("SELECT county,town,apiwellnumber from dataInfo"
                + " WHERE county is not null AND town is not null "
                + "GROUP BY county,town,apiwellnumber");
        result.createOrReplaceTempView("dataInfo5");
    }


    public Dataset<Row> productionForCountyYearly() {
        Dataset<Row> result = spark.sql("SELECT first(id) as id, county, SUM(gasproducedmcf) AS totalgas,"
                + "SUM(waterproducedbbl) AS totalwater,SUM(oilproducedbbl) AS totaloil, reportingyear FROM dataInfo "
                + "where (gasproducedmcf is not null OR waterproducedbbl is not null OR  oilproducedbbl is not null) and"
                + " county is not null GROUP BY county,reportingyear ORDER BY county,reportingyear");

        return result;
    }

    //Return Dataset<Row> that contain rows of production for each Well for the first ReportingYear if parameter is 'false' else return yearly report.
    public Dataset<Row> LocationYearly(boolean condition) {
        Dataset<Row> first_tran = spark.sql("select DISTINCT(newgeoreferencedcolumn),county,reportingyear,SUM(gasproducedmcf), "
                + "SUM(waterproducedbbl),SUM(oilproducedbbl), FIRST(id) as id from dataInfo "
                + "where (gasproducedmcf is not null OR waterproducedbbl is not null OR oilproducedbbl is not null OR newgeoreferencedcolumn is not null) "
                + "GROUP BY newgeoreferencedcolumn,reportingyear,county, id");
        Dataset<String> second_tran = first_tran.map((MapFunction<Row, String>) f ->
                {
                    String temp2 = new String(f.toString());
                    String temp3[] = temp2.split("\\)");

                    String a[] = temp3[0].split("\\(");
                    if (a.length > 1) {
                        String b[] = a[1].split(",");
                        return b[0].trim() + "," + b[1].replace(")]", " ").trim() + "," + a[0].replace("[", "") + temp3[1].replace("]", "");
                    }
                    return null;
                }, Encoders.STRING()
        );
        second_tran.createOrReplaceTempView("dataInfo2");
        Dataset<Row> third_tran = spark.sql("select * from dataInfo2");
        Dataset<Row> four_tran = third_tran
                .withColumn("longitude", split(col("value"), ",").getItem(0))
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
        Dataset<Row> result = spark.sql("select longitude,latitude,county,town,year,gas,water,oil,id FROM dataInfo3");
        if (!condition) {
            result.createOrReplaceTempView("dataInfo4");
            Dataset<Row> fil = spark.sql("select tbl.* from dataInfo4 tbl INNER JOIN ( Select longitude,latitude,MIN(year)"
                    + " MinYear From dataInfo4 GROUP By longitude,latitude )tbl1 ON tbl1.longitude = tbl.longitude"
                    + " AND tbl1.latitude=tbl.latitude Where tbl1.MinYear =tbl.year ORDER BY year DESC");

            return fil;
        }

        return result;
    }

    public Dataset<Row> allCompany() {
        Dataset<Row> result = spark.sql("SELECT DISTINCT(companyname), first(id) as id, reportingyear AS year, SUM(gasproducedmcf) AS gastotal, "
                + "SUM(waterproducedbbl) AS totalwater, SUM(oilproducedbbl) AS totaloil "
                + "from dataInfo GROUP BY companyname,year ORDER BY companyname,year ASC");
        return result;
    }

    public Dataset<Row> townVsWell() {
        Dataset<Row> dataset = spark.sql("SELECT town,COUNT(town)as total_well from dataInfo5 GROUP BY town ORDER BY town ASC");
        return dataset;
    }

    public Dataset<Row> countyVsWell() {
        Dataset<Row> dataset = spark.sql("SELECT county,town,COUNT(county,town)as total_well from dataInfo5"
                + " GROUP BY county,town ORDER BY county,town ASC");
        dataset.createOrReplaceTempView("dataInfo3");
        Dataset<Row> data = spark.sql("SELECT county,SUM(total_well)as total_well from dataInfo3 GROUP BY county ORDER BY county ASC");
        return data;
    }
}