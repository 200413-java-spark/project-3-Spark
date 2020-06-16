package com.github.p3spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

public class SimpleTransform {
	private static Dataset<Row> ds;
	private static Dataset<Row> ds_rename;
	private static SparkSession spark;
	
	public SimpleTransform(SparkSession sp)
	{
		this.ds=sp.read().option("header", true).csv("OaGAP2C.csv");
		this.spark=sp;
		ds_rename=ds.withColumnRenamed("Company Name", "CompanyName").withColumnRenamed("API Well Number","APIWellNumber")
				.withColumnRenamed("API Hole Number","APIHoleNumber").withColumnRenamed("Completion Code", "CompletionCode")
				.withColumnRenamed("Sidetrack Code", "SidetrackCode").withColumnRenamed("Well Type Code", "WellTypeCode")
				.withColumnRenamed("Production Field", "ProductionField").withColumnRenamed("Well Status Code", "WellStatusCode")
				.withColumnRenamed("Well Name", "WellName").withColumnRenamed("Producing Formation", "ProducingFormation")
				.withColumnRenamed("Months in Production", "MonthsinProduction").withColumnRenamed("Gas Produced, Mcf", "GasProducedMcf")
				.withColumnRenamed("Water Produced, bbl", "WaterProducedbbl").withColumnRenamed("Oil Produced, bbl", "OilProducedbbl")
				.withColumnRenamed("Reporting Year", "ReportingYear").withColumnRenamed("New Georeferenced Column", "NewGeoreferencedColumn");
		
		this.ds_rename.createOrReplaceTempView("dataInfo");
	
	}
	//Filter only for a specific company name.
	public static Dataset<Row> filterCompanyName(String company)
	{
		Column comName=ds.col("Company Name");
		Dataset<Row> m=ds.filter(comName.equalTo(company));
		return m;
	}
	//List all distinct company name
	public static Dataset<Row> allCompanyName()
	{
		Dataset<Row> result= spark.sql("SELECT DISTINCT(CompanyName) from dataInfo ORDER BY CompanyName ASC");
		return result;
	}
	//Total oil each county produce
	public static Dataset<Row> oilForCounty()
	{
		Dataset<Row> result=spark.sql("SELECT County,SUM(OilProducedbbl) AS Total_Oil_BBL from dataInfo where OilProducedbbl is not null AND County is not null GROUP BY County ORDER BY County");
		return result;
	}
	//Total water each county produce
	public static Dataset<Row> waterForCounty()
	{
		Dataset<Row> result=spark.sql("SELECT County,SUM(WaterProducedbbl) AS Total_Water_BBL from dataInfo where WaterProducedbbl is not null AND County is not null GROUP BY County ORDER BY County");
		return result;
	}
	//Total gas each county produce
	public static Dataset<Row> gasForCounty()
	{
		Dataset<Row> result=spark.sql("SELECT County,SUM(GasProducedMcf) AS Total_GAS_Mcf from dataInfo where GasProducedMcf is not null AND County is not null GROUP BY County ORDER BY County");
		return result;
	}
	//Total gas,water, oil each county produce
	public static Dataset<Row> productionForCounty()
	{
		Dataset<Row> result=spark.sql("SELECT County,SUM(GasProducedMcf) AS Total_GAS_Mcf,SUM(WaterProducedbbl) AS Total_Water_BBL,SUM(OilProducedbbl) AS Total_Oil_BBL"
				+ " from dataInfo where (GasProducedMcf is not null OR WaterProducedbbl is not null OR  OilProducedbbl is not null) "
				+ "AND County is not null GROUP BY County ORDER BY County");
		return result;
	}
	//Annual total gas,water, oil each county produce 
	public static Dataset<Row> productionForCountyYearly()
	{
		Dataset<Row> result=spark.sql("SELECT County, SUM(GasProducedMcf) AS Total_GAS_Mcf,"
				+ "SUM(WaterProducedbbl) AS Total_Water_BBL,SUM(OilProducedbbl) AS Total_Oil_BBL, ReportingYear FROM dataInfo "
				+ "where (GasProducedMcf is not null OR WaterProducedbbl is not null OR  OilProducedbbl is not null) AND County is not null "
				+ "GROUP BY County,ReportingYear ORDER BY County,ReportingYear");
				
		return result;
	}
	//Current Active Well total for each County
	public static Dataset<Row> activeWellForCounty()
	{
		Dataset<Row> result=spark.sql("Select County, COUNT(WellStatusCode) AS Total_Well from dataInfo where County is not null AND WellStatusCode is not null AND WellStatusCode=='AC'"
				+ " GROUP BY County ORDER BY County");
		return result;
	}
	//Annual Active Well report for each County
	public static Dataset<Row> activeWellForCountyYearly()
	{
		Dataset<Row> result=spark.sql("Select County, COUNT(WellStatusCode) AS Total_Well, ReportingYear from dataInfo where County is not null AND WellStatusCode is not null AND WellStatusCode=='AC'"
				+ " GROUP BY County, ReportingYear ORDER BY County,ReportingYear");
		return result;
	}
	//Return Dataset<Row> that contain rows of production for each Well for the first ReportingYear if parameter is 'false' else return yearly report.
	public static Dataset<Row> LocationYearly(boolean condition)
	{
		Dataset<Row> first_tran=spark.sql("select DISTINCT(NewGeoreferencedColumn),County, ReportingYear,SUM(GasProducedMcf), "
				+ "SUM(WaterProducedbbl),SUM(OilProducedbbl),id from dataInfo "
				+ "where (GasProducedMcf is not null OR WaterProducedbbl is not null OR  OilProducedbbl is not null OR NewGeoreferencedColumn is not null) "
				+ "GROUP BY NewGeoreferencedColumn,ReportingYear,County,id");
		Dataset<String> second_tran=first_tran.map((MapFunction<Row, String>) f -> 
		{
			String temp2=new String(f.toString());
			String temp3[]=temp2.split("\\)");
			String a[]=temp3[0].split("\\(");
			String b[]=a[1].split(",");
			
			return b[0].trim()+","+b[1].replace(")]"," ").trim()+","+a[0].replace("[", "")+temp3[1].replace("]", "");
		},  Encoders.STRING()
		);
		second_tran.createOrReplaceTempView("dataInfo2");
		Dataset<Row> thrid_tran=spark.sql("select * from dataInfo2");
		Dataset<Row> four_tran=thrid_tran.withColumn("longtitude", split(col("value"), ",").getItem(0))
				.withColumn("latitude", split(col("value"), ",").getItem(1))
				.withColumn("town", split(col("value"), ",").getItem(2))
				.withColumn("state", split(col("value"), ",").getItem(3))
				.withColumn("county", split(col("value"), ",").getItem(4))
				.withColumn("year", split(col("value"), ",").getItem(5))
				.withColumn("gas",split(col("value"),",").getItem(6))
				.withColumn("water",split(col("value"),",").getItem(7))
				.withColumn("oil",split(col("value"),",").getItem(8))
				.withColumn("id",split(col("value"),",").getItem(9));
		four_tran.createOrReplaceTempView("dataInfo3");
		Dataset<Row> result=spark.sql("select longtitude,latitude,county,town,year,gas,water,oil,id from dataInfo3");
		if(!condition)
		{
		result.createOrReplaceTempView("dataInfo4");
		Dataset<Row> fil=spark.sql("select tbl.* from dataInfo4 tbl INNER JOIN ( Select longtitude,latitude,MIN(year)"
				+ " MinYear From dataInfo4 GROUP By longtitude,latitude)tbl1 ON tbl1.longtitude = tbl.longtitude AND tbl1.latitude=tbl.latitude Where tbl1.MinYear =tbl.Year"
				+ " ORDER BY year DESC");
			return fil;
		}
		
		return result;
	}
	//Return total of production for all the years.
	public static Dataset<Row> LocationTotal()
	{
		Dataset<Row> dataset=LocationYearly(true);
		dataset.createOrReplaceTempView("dataInfo2");
		Dataset<Row> result=spark.sql("select Long,Lat,County,Town,SUM(cast(Gas as double))Total_GAS,SUM(cast(Water as double)) Total_Water,SUM(cast(Oil as double)) Total_Oil from dataInfo2 GROUP BY Long,Lat,County,Town");
		return result;
		
	}
	
	public static Dataset<Row> CountyProductionYearly()
	{
		Dataset<Row> result=LocationYearly(true);
		result.createOrReplaceTempView("dataInfo2");
		Dataset<Row> dataset=spark.sql("select County,Year,SUM(cast(Gas as double))Total_GAS,SUM(cast(Water as double)) Total_Water,SUM(cast(Oil as double)) Total_Oil from dataInfo2 GROUP BY County,Year ORDER BY Year ASC");
		return dataset;
	}
	public static Dataset<Row> allCompany()
	{
		Dataset<Row> result= spark.sql("SELECT DISTINCT(CompanyName),ReportingYear AS Year,SUM(GasProducedMcf)Total_GAS,SUM(WaterProducedbbl) Total_Water,SUM(OilProducedbbl) Total_Oil from dataInfo GROUP BY CompanyName,Year ORDER BY CompanyName,Year ASC");
		return result;
	}
	public static Dataset<Row> townVsWell()
	{
		Dataset<Row> result=spark.sql("Select County,Town, APIWellNumber from dataInfo where County is not null and Town is not null "
				+ "GROUP BY County,Town,APIWellNumber");
		result.createOrReplaceTempView("dataInfo2");
		Dataset<Row> dataset=spark.sql("Select Town,COUNT(Town)as total_well from dataInfo2 GROUP BY Town ORDER BY Town ASC");
		return dataset;
	}
	public static Dataset<Row> countyVsWell()
	{
		Dataset<Row> result=spark.sql("Select County,Town, APIWellNumber from dataInfo where County is not null and Town is not null "
				+ "GROUP BY County,Town,APIWellNumber");
		result.createOrReplaceTempView("dataInfo2");
		Dataset<Row> dataset=spark.sql("Select County,Town,COUNT(County,Town)as total_well from dataInfo2 GROUP BY County, Town ORDER BY County,Town ASC");
		dataset.createOrReplaceTempView("dataInfo3");
		Dataset<Row> data=spark.sql("Select County,SUM(total_well)as total_well from dataInfo3 GROUP BY County ORDER BY County ASC");
		return data;
		
	}

	
	

	

	

}
