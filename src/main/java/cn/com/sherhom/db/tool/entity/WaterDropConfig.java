package cn.com.sherhom.db.tool.entity;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * @author Sherhom
 * @date 2021/3/22 10:22
 */
public class WaterDropConfig {
    public SparkConfig spark = new SparkConfig();
    public InputConfig input = new InputConfig();
    public FilterConfig filter=new FilterConfig();
    public OutputConfig output=new OutputConfig();

    public static class SparkConfig extends Properties {
        public static final String BATCH_DURATION_KEY = "spark.streaming.batchDuration";
        public static final String APP_NAME = "spark.app.name";
        public static final String UI_PORT = "spark.ui.port";
        public static final String SQL_CATALOG_IMPLEMENTATION = "spark.sql.catalogImplementation";
        public static final String EXECUTOR_INSTANCES = "spark.executor.instances";
        public static final String EXECUTOR_CORES = "spark.executor.cores";
        public static final String EXECUTOR_MEMORY = "spark.executor.memory";

        public SparkConfig() {
            this("WaterDrop-" + System.currentTimeMillis() + "-" + UUID.randomUUID());
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

    public static class InputConfig {
        public static class HIVE {
            public String pre_sql;
            public String table_name = "tmp_table_" + "_" + System.currentTimeMillis() + UUID.randomUUID();

        }

        public HIVE hive = new HIVE();
    }

    public static class FilterConfig {

    }

    public static class OutputConfig {
        public Clickhouse clickhouse;
        public static class Clickhouse {
            public String host;
            public Integer socket_timeout = 50000;
            public String database;
            public String table;
            public List<String> fields;
            public String username;
            public String password;
            public Integer bulk_size = 45000;
        }

    }

}
