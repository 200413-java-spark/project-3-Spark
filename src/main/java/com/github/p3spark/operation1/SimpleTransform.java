package com.github.p3spark.operation1;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SimpleTransform {
    private Dataset<Row> ds;
    private SparkSession spark;

    public SimpleTransform(SparkSession sp, Dataset<Row> data) {
        this.ds = data;
        this.spark = sp;
        ds.createOrReplaceTempView("dataInfo");
    }

    //Filter only for a specific company name.
//	public Dataset<Row> filterCompanyName(String company)
//	{
//		Column comName=ds.col("CompanyName");
//		Dataset<Row> result=ds.filter(comName.equalTo(company));
//		return result;
//	}
//    //List all distinct company name
//    public Dataset<Row> allCompanyName() {
//        Dataset<Row> result = spark.sql("SELECT DISTINCT(CompanyName) from dataInfo ORDER BY CompanyName ASC");
//        return result;
//    }

    //Total oil each county produce
//	public Dataset<Row> oilForCounty()
//	{
//		Dataset<Row> result=spark.sql("SELECT County,SUM(OilProduced_bbl) AS Total_Oil_BBL "
//				+"from dataInfo where OilProduced_bbl is not null AND "
//				+"County is not null GROUP BY County ORDER BY County");
//		return result;
//	}
//	//Total water each county produce
//	public Dataset<Row> waterForCounty()
//	{
//		Dataset<Row> result=spark.sql("SELECT County,SUM(WaterProduced_bbl) AS Total_Water_BBL "
//				+"from dataInfo where WaterProduced_bbl is not null AND "
//				+"County is not null GROUP BY County ORDER BY County");
//		return result;
//	}
//	//Total gas each county produce
//	public Dataset<Row> gasForCounty()
//	{
//		Dataset<Row> result=spark.sql("SELECT County,SUM(GasProduced_Mcf) AS Total_GAS_Mcf from "
//				+"dataInfo where GasProduced_Mcf is not null AND "
//				+"County is not null GROUP BY County ORDER BY County");
//		return result;
//	}
//    //Total gas,water, oil each county produce
//    public Dataset<Row> productionForCounty() {
//        Dataset<Row> result = spark.sql("SELECT County,SUM(GasProduced_Mcf) AS Total_GAS_Mcf,SUM(WaterProduced_bbl) "
//                + "AS Total_Water_BBL,SUM(OilProduced_bbl) AS Total_Oil_BBL from dataInfo where "
//                + "(GasProduced_Mcf is not null OR WaterProduced_bbl is not null OR  OilProduced_bbl is not null) "
//                + "AND County is not null GROUP BY County ORDER BY County");
//        return result;
//    }

    //Annual total gas,water, oil each county produce
    public Dataset<Row> productionForCountyYearly() {
        Dataset<Row> result = spark.sql("SELECT County as county, SUM(GasProduced_Mcf) AS totalgas,"
                + "SUM(WaterProduced_bbl) AS totalwater,SUM(OilProduced_bbl) AS totaloil, ReportingYear as reportingyear FROM dataInfo "
                + "where (GasProduced_Mcf is not null OR WaterProduced_bbl is not null OR  OilProduced_bbl is not null) and"
                + " county is not null GROUP BY county,reportingyear ORDER BY county,reportingyear");

        return result;
    }

//    //Current Active Well total for each County
//    public Dataset<Row> activeWellForCounty() {
//        Dataset<Row> result = spark.sql("Select County, COUNT(WellStatusCode) AS Total_Well from dataInfo where "
//                + "County is not null AND WellStatusCode is not null AND WellStatusCode=='AC' "
//                + "GROUP BY County ORDER BY County");
//        return result;
//    }
//
//    //Annual Active Well report for each County
//    public Dataset<Row> activeWellForCountyYearly() {
//        Dataset<Row> result = spark.sql("Select County, COUNT(WellStatusCode) AS Total_Well, ReportingYear from "
//                + "dataInfo where County is not null AND WellStatusCode is not null AND WellStatusCode=='AC' "
//                + "GROUP BY County, ReportingYear ORDER BY County,ReportingYear");
//        return result;
//    }

    //Return Dataset<Row> that contain rows of production for each Well for the first ReportingYear if parameter is 'false' else return yearly report.
    public Dataset<Row> latlongYearly(boolean condition) {
        Dataset<Row> first_tran = spark.sql("select DISTINCT(NewGeoreferencedColumn),County, ReportingYear,SUM(GasProduced_Mcf), "
                + "SUM(WaterProduced_bbl),SUM(OilProduced_bbl) from dataInfo "
                + "where (GasProduced_Mcf is not null OR WaterProduced_bbl is not null OR  OilProduced_bbl is not null OR NewGeoreferencedColumn is not null) "
                + "GROUP BY NewGeoreferencedColumn,ReportingYear,County");
        Dataset<String> second_tran = first_tran.map((MapFunction<Row, String>) f ->
                {
                    String temp2 = new String(f.toString());
                    String temp3[] = temp2.split("\\)");

                    String a[] = temp3[0].split("\\(");


                    String b[] = a[1].split(",");
                    return b[0].trim() + "," + b[1].replace(")]", " ").trim() + "," + a[0].replace("[", "") + temp3[1].replace("]", "");
                }, Encoders.STRING()
        );
        second_tran.createOrReplaceTempView("dataInfo2");
        Dataset<Row> thrid_tran = spark.sql("select * from dataInfo2");
        Dataset<Row> four_tran = thrid_tran.withColumn("Long", split(col("value"), ",").getItem(0))

                .withColumn("Lat", split(col("value"), ",").getItem(1))
                .withColumn("Town", split(col("value"), ",").getItem(2))
                .withColumn("State", split(col("value"), ",").getItem(3))
                .withColumn("County", split(col("value"), ",").getItem(4))
                .withColumn("Year", split(col("value"), ",").getItem(5))
                .withColumn("Gas", split(col("value"), ",").getItem(6))
                .withColumn("Water", split(col("value"), ",").getItem(7))
                .withColumn("Oil", split(col("value"), ",").getItem(8));
        four_tran.createOrReplaceTempView("dataInfo3");
        Dataset<Row> result = spark.sql("select Long,Lat,County,Town,Year,Gas,Water,Oil from dataInfo3");
        if (!condition) {
            result.createOrReplaceTempView("dataInfo4");
            Dataset<Row> fil = spark.sql("select tbl.* from dataInfo4 tbl INNER JOIN ( Select Long,Lat,MIN(Year)"
                    + " MinYear From dataInfo4 GROUP By Long,Lat )tbl1 ON tbl1.Long = tbl.Long AND tbl1.Lat=tbl.Lat Where tbl1.MinYear =tbl.Year"
                    + " ORDER BY Year DESC");

            return fil;
        }

        return result;
    }

    //	//Return total of production for all the years.
//	public Dataset<Row> LocationTotal()
//	{
//		Dataset<Row> dataset=latlongYearly(true);
//		dataset.createOrReplaceTempView("dataInfo2");
//		Dataset<Row> result=spark.sql("select Long,Lat,County,Town,SUM(cast(Gas as double))Total_GAS,SUM(cast(Water as double)) "
//					+"Total_Water,SUM(cast(Oil as double)) Total_Oil from dataInfo2 GROUP BY Long,Lat,County,Town");
//		return result;
//
//	}
//
//	public Dataset<Row> CountyProductionYearly()
//	{
//		Dataset<Row> result=latlongYearly(true);
//		result.createOrReplaceTempView("dataInfo2");
//		Dataset<Row> dataset=spark.sql("select County,Year,SUM(cast(Gas as double))Total_GAS,SUM(cast(Water as double)) "
//					+"Total_Water,SUM(cast(Oil as double)) Total_Oil from dataInfo2 GROUP BY County,Year ORDER BY Year ASC");
//		return dataset;
//	}
    public Dataset<Row> allCompany() {
        Dataset<Row> result = spark.sql("SELECT DISTINCT(CompanyName),ReportingYear AS Year, SUM(GasProducedMcf), "
                + "SUM(WaterProducedbbl) Total_Water, SUM(OilProducedbbl) Total_Oil "
                + "from dataInfo GROUP BY CompanyName,Year ORDER BY CompanyName,Year ASC");
        return result;
//    }

    }
}
//.productionForCountyYearly();

//.latlongYearly(false);

//.allCompany();