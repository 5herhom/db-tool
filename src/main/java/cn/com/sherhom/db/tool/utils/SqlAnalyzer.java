package cn.com.sherhom.db.tool.utils;

import cn.com.sherhom.db.tool.entity.ColumnInfo;

import java.util.List;

/**
 * @author Sherhom
 * @date 2021/4/9 9:48
 */
public interface SqlAnalyzer {
    void loadByAnalyzer(SqlAnalyzer sqlAnalyzer);
    String database();
    String table();
    List<ColumnInfo> columnInfos();
    List<ColumnInfo> partitionsInfos();
    String tableComment();
    void database(String database);
    void table(String table);
    void columnInfos(List<ColumnInfo> columnInfos);
    void partitions(List<ColumnInfo> partitions);
    void tableComment(String tableComment);
    static boolean typeEquals(String type01,String type02){
        if(type01==null||type02==null){
            return false;
        }
        return type01.equalsIgnoreCase(type02);
    }
}
