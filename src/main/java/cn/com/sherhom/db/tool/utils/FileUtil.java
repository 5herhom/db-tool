package cn.com.sherhom.db.tool.utils;

import java.text.MessageFormat;

/**
 * @author Sherhom
 * @date 2021/4/12 11:40
 */
public class FileUtil {
    public static final String SQL_FILE_PATTERN="file:data/hive-sql/{0}.sql";
    public static String getSqlFile(String tableString){
        return MessageFormat.format(SQL_FILE_PATTERN,tableString);
    }
}
