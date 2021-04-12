package cn.com.sherhom.db.tool.utils;

import cn.com.sherhom.db.tool.entity.ColumnInfo;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Sherhom
 * @date 2021/4/9 16:46
 */
public abstract class AbstractSqlAnalyzer implements SqlAnalyzer {
    String database;
    String table;
    List<ColumnInfo> columnInfos;
    List<ColumnInfo> partitions;
    String tableComment;
    public static final String CREATE_HEAD = "create";
    public static final String CREATE_TABLE = "table";
    public static final String COMMENT_KEY = "COMMENT";
    public static final Character COL_SEPARATOR = ',';
    public static final Character SPACE = ' ';
    public static final Character SQL_ENDING = ';';
    public static final Character SINGLE_QUATATIONS = '\'';
    public static final Character DOUBLE_QUATATIONS = '"';
    public static final List<Character> LINE_FEEDS= Stream.of('\n','\r').collect(Collectors.toList());
    public static final List<Character> QUATATIONS= Stream.of('"','\'').collect(Collectors.toList());
    public static final List<Character> SKIP_CHAR= Stream.of('\n','\r',' ').collect(Collectors.toList());
    @Override
    public void loadByAnalyzer(SqlAnalyzer analyzer){
        this.database(analyzer.database());
        this.table(analyzer.table());
        this.columnInfos(analyzer.columnInfos());
        this.partitions(analyzer.partitionsInfos());
        this.tableComment(analyzer.tableComment());
    }

    @Override
    public String database() {
        return this.database;
    }

    @Override
    public String table() {
        return this.table;
    }

    @Override
    public List<ColumnInfo> columnInfos() {
        return this.columnInfos;
    }

    @Override
    public List<ColumnInfo> partitionsInfos() {
        return this.partitions;
    }

    @Override
    public String tableComment() {
        return this.tableComment;
    }

    @Override
    public void database(String database) {
        this.database =database;
    }

    @Override
    public void table(String table) {
        this.table=table;
    }

    @Override
    public void columnInfos(List<ColumnInfo> columnInfos) {
        this.columnInfos=columnInfos;
    }

    @Override
    public void partitions(List<ColumnInfo> columnInfos) {
        this.partitions=columnInfos;
    }

    @Override
    public void tableComment(String tableComment){
        this.tableComment=tableComment;
    }

}
