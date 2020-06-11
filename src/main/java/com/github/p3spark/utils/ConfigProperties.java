package com.github.p3spark.utils;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ConfigProperties {

    Properties properties = new Properties();
    String FileName = "application.properties";
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(FileName);

    public ConfigProperties() {
        loadProperties();
    }


    private void loadProperties() {
        try {
            properties.load(inputStream);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public String getDriver() {
        return properties.getProperty("driver");
    }

    public String getDbtable() {
        return properties.getProperty("dbtable");
    }

    public String getUser() {
        return properties.getProperty("user");
    }

    public String getPassword() {
        return properties.getProperty("password");
    }

    public String getKafkaurl() {
        return properties.getProperty("kafkaurl");
    }

    public String getKafkatopic() {
        return properties.getProperty("kafkatopic");
    }
}