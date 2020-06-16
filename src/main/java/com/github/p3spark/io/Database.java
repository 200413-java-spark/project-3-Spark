package com.github.p3spark.io;

import com.github.p3spark.utils.ConfigProperties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class Database {

    ConfigProperties configProperties = new ConfigProperties();
    
    String dburl = configProperties.getUrl();
    String dbusername = configProperties.getUser();
    String dbpassword = configProperties.getPassword();
    String driver = configProperties.getDriver();
    String table;

    public Database(){}

    public void writeToDatabase(Dataset<Row> dataIn, int whichTable){
                // Saving data to a JDBC source
                switch (whichTable) {
                    case 1:
                    table = configProperties.getDbtable1();
                        break;
                    case 2:
                    table = configProperties.getDbtable2();
                        break;
                    case 3:
                    table = configProperties.getDbtable3();
                        break;
                    case 4:
                    table = configProperties.getDbtable4();
                        break;
                    case 5:
                    table = configProperties.getDbtable5();
                        break;
                    default:
                        break;
                }
                
                dataIn.write().format("jdbc")
                .option("url", dburl)
                .option("driver", driver)
                .option("dbtable", table)
                .option("user", dbusername)
                .option("password", dbpassword)
                .mode(SaveMode.Overwrite)
                .save();
    }
}