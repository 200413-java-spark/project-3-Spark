package com.github.p3spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleTransform {
	private static Dataset<Row> ds;
	private static Dataset<Row> ds_rename;
	private static SparkSession spark;
	
	public SimpleTransform(SparkSession sp)
	{
		this.ds=sp.read().option("header", true).csv("Oil.csv");
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
		ds_rename.printSchema();
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
	
	

	

}
