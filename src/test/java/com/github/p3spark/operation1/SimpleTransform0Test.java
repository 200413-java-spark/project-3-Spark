package com.github.p3spark.operation1;

import com.github.p3spark.startup.CreateSparkSession;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class SimpleTransform0Test {
    SimpleTransform0 simpleTransform0;

    @BeforeEach
    void setUp() {
        CreateSparkSession startSession = CreateSparkSession.getInstance();
        SparkSession session = startSession.getSession();
        simpleTransform0 = new SimpleTransform0(session);
    }

    @Test
    @Disabled
    @DisplayName("Testing avgMonthsInProduction method")
    void avgMonthsInProduction() {
        simpleTransform0.avgMonthsInProduction();
    }

    @Test
    @Disabled
    @DisplayName("Testing totalWaterProduced method")
    void totalWaterProduced() {
        simpleTransform0.totalWaterProduced();
    }

    @Test
    @Disabled
    @DisplayName("Testing totalGasProduced method")
    void totalGasProduced() {
        simpleTransform0.totalGasProduced();
    }

    @Test
    @Disabled
    @DisplayName("Testing completionCodeSummary method")
    void completionCodeSummary() {
        simpleTransform0.completionCodeSummary();
    }

    @Test
    @Disabled
    @DisplayName("Testing wellTypeCodeSummary method")
    void wellTypeCodeSummary() {
        simpleTransform0.wellTypeCodeSummary();
    }

    @Test
    @Disabled
    @DisplayName("Testing sidetrackCodeSummary method")
    void sidetrackCodeSummary() {
        simpleTransform0.sidetrackCodeSummary();
    }
}