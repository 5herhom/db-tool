package cn.com.sherhom.db.tool.utils;

import cn.com.sherhom.reno.common.exception.RenoException;
import cn.com.sherhom.reno.common.loader.URLLoader;
import cn.com.sherhom.reno.common.loader.URLLoaderFactory;
import cn.com.sherhom.reno.common.utils.Asset;
import cn.com.sherhom.reno.common.utils.ConfUtil;
import cn.com.sherhom.reno.common.utils.LogUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Sherhom
 * @date 2020/12/31 11:28
 */
@Slf4j
public class SqlReadUtil {
    public static final String SQL_SEPARATOR = ";";
    public static final String BLANK = " ";
    public static final String PLACE_HOLDER_START = "${";
    public static final String PLACE_HOLDER_END = "}";

    public static List<String> readSql(String path) {
        URLLoader urlLoader = URLLoaderFactory.getInstance(path);
        InputStream inputStream = null;
        BufferedReader bf = null;
        List<String> sqlList = new ArrayList<>();
        try {
            inputStream = urlLoader.getInStream();
            bf = new BufferedReader(new InputStreamReader(inputStream));
            String line, front, back;
            StringBuilder sqlSb = new StringBuilder();
            int indexOfSeparator, lastIndexOfSeparator;
            while ((line = bf.readLine()) != null) {
                lastIndexOfSeparator = -1;
                indexOfSeparator = line.indexOf(SQL_SEPARATOR);
                while (indexOfSeparator != -1) {
                    front = line.substring(lastIndexOfSeparator + 1, indexOfSeparator);
                    sqlSb.append(front);
                    sqlList.add(sqlSb.toString());
                    sqlSb = new StringBuilder();
                    lastIndexOfSeparator = indexOfSeparator;
                    indexOfSeparator = line.indexOf(SQL_SEPARATOR, indexOfSeparator + 1);
                }
                if (lastIndexOfSeparator + 1 != line.length())
                    sqlSb.append(line.substring(lastIndexOfSeparator + 1)).append(BLANK);
            }
            if (sqlSb != null && sqlSb.length() > 0) {
                sqlList.add(sqlSb.toString());
            }
        } catch (Exception e) {
            log.error("Read sql in path [{}] failed.", path);
            LogUtil.printStackTrace(e);
            throw new RenoException(e);
        } finally {
            IOUtils.closeQuietly(bf);
        }
        return replaceValueForSqlList(sqlList);
    }

    public static final String REPLACE_ERROR_MESSAGE_FORMAT = "Sql {0} ,not format index {1},when replaceValue.";

    public static List<String> replaceValueForSqlList(List<String> sqlList) {
        return replaceValueForSqlList(sqlList, key -> ConfUtil.get(key));
    }

    public static List<String> replaceValueForSqlList(List<String> sqlList, Function<String, String> key2Value) {
        return sqlList.stream().map(sql -> replaceValue(sql, key2Value)).collect(Collectors.toList());
    }

    public static String replaceValue(String sql, Function<String, String> key2Value) {
        int i = 0, placeHolderStartIndex, placeHolderEndIndex = -1, keyStartIndex;
        int sqlLen = sql.length();
        StringBuilder sqlSb = new StringBuilder(sql);
        placeHolderStartIndex = sql.indexOf(PLACE_HOLDER_START, placeHolderEndIndex + 1);
        String key, replaceStr, value;
        String newSql;
        while (placeHolderStartIndex >= 0) {
            sqlLen = sqlSb.length();
            keyStartIndex = placeHolderStartIndex + PLACE_HOLDER_START.length();
            Asset.isTrue(keyStartIndex < sqlLen, replaceErrorMessage(sql, placeHolderStartIndex));
            placeHolderEndIndex = sqlSb.indexOf(PLACE_HOLDER_END, keyStartIndex);
            Asset.isTrue(placeHolderEndIndex >= 0, replaceErrorMessage(sql, placeHolderStartIndex));
            key = sqlSb.substring(keyStartIndex, placeHolderEndIndex);
            value = key2Value.apply(key);
            Asset.isNotBlank(value, replaceNoValueErrorMessage(sql, key));
            sqlSb.replace(placeHolderStartIndex, placeHolderEndIndex + 1, value);
            placeHolderEndIndex = placeHolderEndIndex + (value.length() - placeHolderString(key).length());
            placeHolderStartIndex = sqlSb.indexOf(PLACE_HOLDER_START, placeHolderEndIndex + 1);
        }
        return sqlSb.toString();
    }

    public static String placeHolderString(String key) {
        return PLACE_HOLDER_START + key + PLACE_HOLDER_END;
    }

    public static String replaceErrorMessage(String sql, int index) {
        return MessageFormat.format("Sql {0} ,not format index {1},when replaceValue.", sql, index);
    }

    public static String replaceNoValueErrorMessage(String sql, String key) {
        return MessageFormat.format("Sql {0} , has no value key [{1}].", sql, key);
    }
}
