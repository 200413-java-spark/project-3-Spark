package com.github.p3spark.operation1;

import com.github.p3spark.startup.CreateSparkSession;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
    @DisplayName("Testing avgMonthsInProduction method")
    void avgMonthsInProduction() {
        simpleTransform0.avgMonthsInProduction();
    }

    @Test
    @DisplayName("Testing totalWaterProduced method")
    void totalWaterProduced() {
        simpleTransform0.totalWaterProduced();
    }

    @Test
    @DisplayName("Testing totalGasProduced method")
    void totalGasProduced() {
        simpleTransform0.totalGasProduced();
    }

    @Test
    @DisplayName("Testing completionCodeSummary method")
    void completionCodeSummary() {
        simpleTransform0.completionCodeSummary();
    }

    @Test
    @DisplayName("Testing wellTypeCodeSummary method")
    void wellTypeCodeSummary() {
        simpleTransform0.wellTypeCodeSummary();
    }

    @Test
    @DisplayName("Testing sidetrackCodeSummary method")
    void sidetrackCodeSummary() {
        simpleTransform0.sidetrackCodeSummary();
    }
}