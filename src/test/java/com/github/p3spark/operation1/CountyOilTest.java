package com.github.p3spark.operation1;

import com.github.p3spark.startup.CreateSparkSession;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CountyOilTest {
    SparkSession session;
    CountyOil countyOil;

    @BeforeEach
    void setUp() {
        CreateSparkSession startSession = CreateSparkSession.getInstance();
        session = startSession.getSession();
        countyOil = new CountyOil();
    }

    @Test
    void findTotOilByCounty() throws InterruptedException {
        countyOil.findTotOilByCounty(session);
    }
}