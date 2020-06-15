package com.github.p3spark.utils;

import com.github.p3spark.startup.Consumer;
import com.github.p3spark.startup.CreateSparkSession;
import org.apache.spark.sql.SparkSession;

public class Init {
    CreateSparkSession startSession;
    SparkSession session;
    public Init(){
        startSpark();
        endSpark();
    }

    private void startSpark(){
         startSession = CreateSparkSession.getInstance();
         session = startSession.getSession();
        new Consumer().builder(session);
    }

    private void endSpark(){
        session.close();
    }
}
