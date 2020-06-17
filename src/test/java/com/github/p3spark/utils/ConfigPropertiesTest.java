package com.github.p3spark.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigPropertiesTest {
    ConfigProperties configProperties = new ConfigProperties();


    @Test
    @DisplayName("Testing Driver getter")
    void getDriver() {
        String a = "org.postgresql.Driver";
        String b = configProperties.getDriver();
        assertSame(a, b);
    }

    @Test
    @DisplayName("Testing url getter")
    void getUrl() {
        String a = "jdbc:postgresql://3.17.207.114:5432/mydb";
        String b = configProperties.getUrl();
        assertSame(a, b);
    }

    @Test
    @DisplayName("Testing Dbtable1 getter")
    void getDbtable1() {
        String a = "countyvsoilproductionbyyear";
        String b = configProperties.getDbtable1();
        assertSame(a, b);
    }

    @Test
    @DisplayName("Testing Dbtable2 getter")
    void getDbtable2() {
        String a = "locationyearly";
        String b = configProperties.getDbtable2();
        assertSame(a, b);
    }

    @Test
    @DisplayName("Testing Dbtable3 getter")
    void getDbtable3() {
        String a = "allcompany";
        String b = configProperties.getDbtable3();
        assertSame(a, b);
    }

    @Test
    @DisplayName("Testing Dbtable4 getter")
    void getDbtable4() {
        String a = "townvswell";
        String b = configProperties.getDbtable4();
        assertSame(a, b);
    }

    @Test
    @DisplayName("Testing Dbtable5 getter")
    void getDbtable5() {
        String a = "countyvswell";
        String b = configProperties.getDbtable5();
        assertSame(a, b);
    }

    @Test
    @DisplayName("Testing User getter")
    void getUser() {
        String a = "mydb";
        String b = configProperties.getUser();
        assertSame(a, b);
    }

    @Test
    @DisplayName("Testing Password getter")
    void getPassword() {
        String a = "mydb";
        String b = configProperties.getPassword();
        assertSame(a, b);
    }

    @Test
    @DisplayName("Testing Kafkaurl getter")
    void getKafkaurl() {
        String a = "3.16.158.213:9092";
        String b = configProperties.getKafkaurl();
        assertSame(a, b);
    }

    @Test
    @DisplayName("Testing Kafkatopic getter")
    void getKafkatopic() {
        String a = "oil";
        String b = configProperties.getKafkatopic();
        assertSame(a, b);
    }
}