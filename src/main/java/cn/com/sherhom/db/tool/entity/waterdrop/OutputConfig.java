package cn.com.sherhom.db.tool.entity.waterdrop;

import java.util.List;

/**
 * @author Sherhom
 * @date 2021/4/12 14:39
 */
public class OutputConfig {
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
