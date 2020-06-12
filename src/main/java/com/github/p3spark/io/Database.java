package com.github.p3spark.io;

import com.github.p3spark.utils.ConfigProperties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class Database {

    public Database(){}
    ConfigProperties configProperties = new ConfigProperties();

    String dburl = configProperties.getUrl();
    String dbusername = configProperties.getUser();
    String dbpassword = configProperties.getPassword();
    String driver = configProperties.getDriver();

    public void writeToDatabase(SparkSession session, Dataset<Row> data, String tableName)
    {
        // Saving data to a JDBC source
        data.write()
        .format("jdbc")
        .mode(SaveMode.Append)
        .option("url", dburl)
        .option("driver", driver)
        .option("dbtable", tableName)
        .option("user", dbusername)
        .option("password", dbpassword)
        .save();
    }

    public Dataset<Row> readFromDatabase(SparkSession session,String tableName)
    {
        Dataset<Row> jdbcDF = session.read()
            .format("jdbc")
            .option("url", dburl)
            .option("dbtable", tableName)
            .option("user", dbusername)
            .option("password", dbpassword)
            .load();
        return jdbcDF;

}
}