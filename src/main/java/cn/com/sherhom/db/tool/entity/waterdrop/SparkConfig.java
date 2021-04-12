package cn.com.sherhom.db.tool.entity.waterdrop;

import java.util.Properties;
import java.util.UUID;

/**
 * @author Sherhom
 * @date 2021/4/12 14:39
 */
public class SparkConfig extends Properties {
    public static final String BATCH_DURATION_KEY = "spark.streaming.batchDuration";
    public static final String APP_NAME = "spark.app.name";
    public static final String UI_PORT = "spark.ui.port";
    public static final String SQL_CATALOG_IMPLEMENTATION = "spark.sql.catalogImplementation";
    public static final String EXECUTOR_INSTANCES = "spark.executor.instances";
    public static final String EXECUTOR_CORES = "spark.executor.cores";
    public static final String EXECUTOR_MEMORY = "spark.executor.memory";

    public SparkConfig() {
        this("WaterDrop_" + System.currentTimeMillis() + "_" + UUID.randomUUID());
    }

    public SparkConfig(String appName) {
        put(BATCH_DURATION_KEY, 5);
        put(APP_NAME, appName);
        put(UI_PORT, 13000);
        put(SQL_CATALOG_IMPLEMENTATION, "hive");
        put(EXECUTOR_INSTANCES, 50);
        put(EXECUTOR_CORES, 2);
        put(EXECUTOR_MEMORY, "2g");
    }

}
