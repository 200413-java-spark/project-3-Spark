package com.github.p3spark.utils;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ConfigProperties {

    public ConfigProperties() {

    }

    public String getDriver() {
        return "org.postgresql.Driver";
    }

    public String getUrl() {
        return "jdbc:postgresql://3.17.207.114:5432/mydb";
    }

    public String getDbtable1() {
        return "countyvsoilproductionbyyear";
    }

    public String getDbtable2() {
        return "latlongyearly";
    }

    public String getDbtable3() {
        return "allcompany";
    }

    public String getUser() {
        return "mydb";
    }

    public String getPassword() {
        return "mydb";
    }

    public String getKafkaurl() {
        return "3.16.158.213:9092";
    }

    public String getKafkatopic() {
        return "oil";
    }
}