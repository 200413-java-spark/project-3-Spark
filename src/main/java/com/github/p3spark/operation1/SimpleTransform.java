package com.github.p3spark.operation1;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleTransform {
	private Dataset<Row> ds;
	//private Dataset<Row> ds_rename;
	private SparkSession spark;
	
	public SimpleTransform(SparkSession sp, Dataset<Row> data)
	{
		this.ds=data;
		this.spark=sp;
		/*ds_rename=ds.withColumnRenamed("Company Name", "CompanyName").withColumnRenamed("API Well Number","APIWellNumber")
				.withColumnRenamed("API Hole Number","APIHoleNumber").withColumnRenamed("Completion Code", "CompletionCode")
				.withColumnRenamed("Sidetrack Code", "SidetrackCode").withColumnRenamed("Well Type Code", "WellTypeCode")
				.withColumnRenamed("Production Field", "ProductionField").withColumnRenamed("Well Status Code", "WellStatusCode")
				.withColumnRenamed("Well Name", "WellName").withColumnRenamed("Producing Formation", "ProducingFormation")
				.withColumnRenamed("Months in Production", "MonthsinProduction").withColumnRenamed("Gas Produced, Mcf", "GasProduced,Mcf")
				.withColumnRenamed("Water Produced, bbl", "WaterProduced,bbl").withColumnRenamed("Oil Produced, bbl", "OilProduced,bbl")
				.withColumnRenamed("Reporting Year", "ReportingYear").withColumnRenamed("New Georeferenced Column", "NewGeoreferencedColumn");*/
		
		ds.createOrReplaceTempView("dataInfo");
		ds.printSchema();
	}
	//Filter only for a specific company name.
	public Dataset<Row> filterCompanyName(String company)
	{
		Column comName=ds.col("CompanyName");
		Dataset<Row> m=ds.filter(comName.equalTo(company));
		return m;
	}
	//List all distinct company name
	public Dataset<Row> allCompanyName()
	{
		Dataset<Row> result= spark.sql("SELECT DISTINCT(CompanyName) from dataInfo ORDER BY CompanyName ASC");
		return result;
	}
	
	//Total oil each county produce
	public Dataset<Row> oilForCounty()
	{
		Dataset<Row> result=spark.sql("SELECT County,SUM(OilProducedbbl) AS Total_Oil_BBL from dataInfo where OilProduced_bbl is not null AND County is not null GROUP BY County ORDER BY County");
		return result;
	}
	//Total water each county produce
	public Dataset<Row> waterForCounty()
	{
		Dataset<Row> result=spark.sql("SELECT County,SUM(WaterProducedbbl) AS Total_Water_BBL from dataInfo where WaterProduced_bbl is not null AND County is not null GROUP BY County ORDER BY County");
		return result;
	}
	//Total gas each county produce
	public Dataset<Row> gasForCounty()
	{
		Dataset<Row> result=spark.sql("SELECT County,SUM(GasProducedMcf) AS Total_GAS_Mcf from dataInfo where GasProduced_Mcf is not null AND County is not null GROUP BY County ORDER BY County");
		return result;
	}
	//Total gas,water, oil each county produce
	public Dataset<Row> productionForCounty()
	{
		Dataset<Row> result=spark.sql("SELECT County,SUM(GasProducedMcf) AS Total_GAS_Mcf,SUM(WaterProduced_bbl) AS Total_Water_BBL,SUM(OilProduced_bbl) AS Total_Oil_BBL"
				+ " from dataInfo where (GasProduced_Mcf is not null OR WaterProduced_bbl is not null OR  OilProduced_bbl is not null) "
				+ "AND County is not null GROUP BY County ORDER BY County");
		return result;
	}
	//Annual total gas,water, oil each county produce 
	public Dataset<Row> productionForCountyYearly()
	{
		Dataset<Row> result=spark.sql("SELECT County, SUM(GasProduced_Mcf) AS Total_GAS_Mcf,"
				+ "SUM(WaterProduced_bbl) AS Total_Water_BBL,SUM(OilProduced_bbl) AS Total_Oil_BBL, ReportingYear FROM dataInfo "
				+ "where (GasProduced_Mcf is not null OR WaterProduced_bbl is not null OR  OilProduced_bbl is not null) AND County is not null "
				+ "GROUP BY County,ReportingYear ORDER BY County,ReportingYear");
				
		return result;
	}
	//Current Active Well total for each County
	public Dataset<Row> activeWellForCounty()
	{
		Dataset<Row> result=spark.sql("Select County, COUNT(WellStatusCode) AS Total_Well from dataInfo where County is not null AND WellStatusCode is not null AND WellStatusCode=='AC'"
				+ " GROUP BY County ORDER BY County");
		return result;
	}
	//Annual Active Well report for each County
	public Dataset<Row> activeWellForCountyYearly()
	{
		Dataset<Row> result=spark.sql("Select County, COUNT(WellStatusCode) AS Total_Well, ReportingYear from dataInfo where County is not null AND WellStatusCode is not null AND WellStatusCode=='AC'"
				+ " GROUP BY County, ReportingYear ORDER BY County,ReportingYear");
		return result;
	}
	

}
