package com.github.p3spark.operation1;

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
				.withColumnRenamed("Months in Production", "MonthsinProduction").withColumnRenamed("Gas Produced, Mcf", "GasProduced,Mcf")
				.withColumnRenamed("Water Produced, bbl", "WaterProduced,bbl").withColumnRenamed("Oil Produced, bbl", "OilProduced,bbl")
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
	

	

}
