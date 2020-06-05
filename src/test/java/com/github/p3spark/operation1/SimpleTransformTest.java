package com.github.p3spark.operation1;

import com.github.p3spark.startup.CreateSparkSession;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SimpleTransformTest {
    SimpleTransform simpleTransform;

    @BeforeEach
    void setUp() {
        CreateSparkSession startSession = CreateSparkSession.getInstance();
        SparkSession session = startSession.getSession();
        simpleTransform = new SimpleTransform(session);
    }

    @Test
    @DisplayName("Testing filterCompanyName method")
    void filterCompanyName() {
        SimpleTransform.filterCompanyName("Klein Oil & Logging");
    }

    @Test
    @DisplayName("Testing allCompanyName method")
    void allCompanyName() {
        SimpleTransform.allCompanyName();
    }
}