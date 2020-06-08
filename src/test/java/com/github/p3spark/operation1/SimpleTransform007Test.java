package com.github.p3spark.operation1;

import com.github.p3spark.startup.CreateSparkSession;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SimpleTransform007Test {
    SimpleTransform007 simpleTransform007;

    @BeforeEach
    void setUp() {
        CreateSparkSession startSession = CreateSparkSession.getInstance();
        SparkSession session = startSession.getSession();
        simpleTransform007 = new SimpleTransform007(session);
    }

    @Test
    void avgMonthsInProduction() {
        simpleTransform007.avgMonthsInProduction();
    }

    @Test
    void totalWaterProduced() {
        simpleTransform007.totalWaterProduced();
    }

    @Test
    void totalGasProduced() {
        simpleTransform007.totalGasProduced();
    }

    @Test
    void completionCodeSummary() {
        simpleTransform007.completionCodeSummary();
    }

    @Test
    void wellTypeCodeSummary() {
        simpleTransform007.wellTypeCodeSummary();
    }

    @Test
    void sidetrackCodeSummary() {
        simpleTransform007.sidetrackCodeSummary();
    }
}