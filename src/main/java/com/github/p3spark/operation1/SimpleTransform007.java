package com.github.p3spark.operation1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleTransform007 {
    private static Dataset<Row> ds;
    private static Dataset<Row> ds_rename;
    private static SparkSession spark;

    public SimpleTransform007(SparkSession sparkSession) {
        ds = sparkSession.read().option("header", true).csv("Oil.csv");
        spark = sparkSession;
        ds_rename = ds.withColumnRenamed("Company Name", "CompanyName")
                .withColumnRenamed("API Well Number", "APIWellNumber")
                .withColumnRenamed("API Hole Number", "APIHoleNumber")
                .withColumnRenamed("Completion Code", "CompletionCode")
                .withColumnRenamed("Sidetrack Code", "SidetrackCode")
                .withColumnRenamed("Well Type Code", "WellTypeCode")
                .withColumnRenamed("Production Field", "ProductionField")
                .withColumnRenamed("Well Status Code", "WellStatusCode")
                .withColumnRenamed("Well Name", "WellName")
                .withColumnRenamed("Producing Formation", "ProducingFormation")
                .withColumnRenamed("Months in Production", "MonthsinProduction")
                .withColumnRenamed("Gas Produced, Mcf", "GasProducedMcf")
                .withColumnRenamed("Water Produced, bbl", "WaterProduced")
                .withColumnRenamed("Oil Produced, bbl", "OilProduced,bbl")
                .withColumnRenamed("Reporting Year", "ReportingYear")
                .withColumnRenamed("New Georeferenced Column", "NewGeoreferencedColumn");

        ds_rename.createOrReplaceTempView("dataInfo");
    }

    public void avgMonthsInProduction() {
        Dataset<Row> result= spark.sql("select CompanyName, format_number( avg(MonthsinProduction), 2 ) as Average_Months_in_Production from dataInfo group by CompanyName order by Average_Months_in_Production desc");
         result.show();
    }

    public void totalWaterProduced() {
        Dataset<Row> result= spark.sql("select CompanyName, format_number( sum(WaterProduced), 2 ) as Total_Water_Produced from dataInfo group by CompanyName order by Total_Water_Produced desc");
        result.show();
    }

    public void totalGasProduced() {
        Dataset<Row> result= spark.sql("select CompanyName, format_number( sum(GasProducedMcf), 2 ) as Total_Gas_Produced from dataInfo group by CompanyName order by Total_Gas_Produced desc");
        result.show();
    }

    public void completionCodeSummary() {
        Dataset<Row> result= spark.sql("select CompletionCode, count(CompletionCode) as Summary from dataInfo group by CompletionCode order by Summary desc ");
        result.show();
    }
    public void wellTypeCodeSummary() {
        Dataset<Row> result= spark.sql("select WellTypeCode, count(WellTypeCode) as Summary from dataInfo group by WellTypeCode order by Summary desc ");
        result.show();
    }

    public void sidetrackCodeSummary() {
        Dataset<Row> result= spark.sql("select SidetrackCode, count(SidetrackCode) as Summary from dataInfo group by SidetrackCode order by Summary desc ");
        result.show();
    }
}
