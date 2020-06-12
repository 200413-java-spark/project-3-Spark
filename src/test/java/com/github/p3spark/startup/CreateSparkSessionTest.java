package com.github.p3spark.startup;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CreateSparkSessionTest {
    CreateSparkSession startSession = CreateSparkSession.getInstance();

    @Test
    @DisplayName("Testing getInstance method")
    void getInstance() {

    }

    @Test
    @DisplayName("Testing getSession method")
    void getSession() {
        SparkSession session = startSession.getSession();
        session.close();
    }
}