package cn.com.sherhom.db.tool.utils;

import cn.com.sherhom.db.tool.entity.CloseEntity;
import cn.com.sherhom.db.tool.entity.ColumnInfo;
import cn.com.sherhom.db.tool.entity.Type;
import cn.com.sherhom.reno.common.utils.Asset;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Sherhom
 * @date 2021/4/9 17:03
 */
public class HiveSqlAnalyzer extends AbstractSqlAnalyzer {

    public void loadFromCreateSql(String createSql) {
        int columnLeftIndex = createSql.indexOf("(");
        String headSql = createSql.substring(0, columnLeftIndex);
        int i = 0;
        char c;
        while (i < headSql.length()) {
            i = skip(headSql, i, SKIP_CHAR);
            if (headSql.length() - i >= CREATE_HEAD.length()
                    && headSql.substring(i, i + CREATE_HEAD.length()).equalsIgnoreCase(CREATE_HEAD)) {
                i += CREATE_HEAD.length();
                continue;
            } else if (headSql.length() - i >= CREATE_TABLE.length() &&
                    headSql.substring(i, i + CREATE_TABLE.length()).equalsIgnoreCase(CREATE_TABLE)) {
                i += CREATE_TABLE.length();
                i = skip(headSql, i, SKIP_CHAR);
                StringBuilder tableBuilder = new StringBuilder();
                boolean isInClose = (headSql.charAt(i++)) == HIVE_ESCAPE_CHARACTOR;
                while ((c = headSql.charAt(i)) != HIVE_ESCAPE_CHARACTOR
                        && c != SPACE && c != '(') {
                    tableBuilder.append(c);
                    i++;
                }
                Asset.isTrue((isInClose && c == HIVE_ESCAPE_CHARACTOR) || c != HIVE_ESCAPE_CHARACTOR, ERROR_PATTERN, i);
                String tableString = tableBuilder.toString();
                int splitIndex;
                if ((splitIndex = tableString.indexOf('.')) != -1) {
                    this.database = tableString.substring(0, splitIndex);
                    this.table = tableString.substring(splitIndex + 1);
                } else {
                    this.table = tableString;
                }
            }
            i++;
        }
        i = columnLeftIndex + 1;
        Result<List<ColumnInfo>> result = analyzeColumns(createSql, i);
        List<ColumnInfo> columns = result.message;
        i += result.offset + 1;
        List<ColumnInfo> partitions = new ArrayList<>();
        String tableComment;
        while (i < createSql.length()) {
            i = skip(createSql, i, SKIP_CHAR);
            if (createSql.length() - i >= HIVE_PARTITION_FIRST.length()
                    && createSql.substring(i, i + HIVE_PARTITION_FIRST.length()).equalsIgnoreCase(HIVE_PARTITION_FIRST)) {
                i += HIVE_PARTITION_FIRST.length();
                i = skip(createSql, i, SKIP_CHAR);
                i += checkField(createSql, i, HIVE_PARTITION_SECOND);
                i = skip(createSql, i, SKIP_CHAR);
                Asset.isTrue('('==createSql.charAt(i),ERROR_PATTERN,i);
                i++;
                Result<List<ColumnInfo>> partitionResult=analyzeColumns(createSql,i);
                partitions=partitionResult.message;
                i+=partitionResult.offset;
            }
            else if(createSql.length()-1>=COMMENT_KEY.length()
                    &&createSql.substring(i,i+COMMENT_KEY.length()).equalsIgnoreCase(COMMENT_KEY)){
                Result<String> commentRes=analyzeComment(createSql,i);
                tableComment=commentRes.message;
                this.tableComment=tableComment;
                i+=commentRes.offset;
            }
            else{
                while(i<createSql.length()&&!LINE_FEEDS.contains(c=createSql.charAt(i)))i++;
                continue;
            }
            i++;
        }
        this.partitions=partitions;
        this.columnInfos=columns;
    }

    public static final String HIVE_PARTITION_FIRST = "PARTITIONED";
    public static final String HIVE_PARTITION_SECOND = "BY";
    public static final Character HIVE_ESCAPE_CHARACTOR = '`';

    public static final boolean isQuatation(char c) {
        for (Character character : QUATATIONS) {
            if (character.equals(c))
                return true;
        }
        return false;
    }

    public static Result<List<ColumnInfo>> analyzeColumns(String sql, int start) {
        int i = start;
        List<ColumnInfo> columns = new ArrayList<>();
        ColumnInfo column = new ColumnInfo();
        char c;
        for (; i < sql.length() && (c = sql.charAt(i)) != ')'; ) {
            if (COL_SEPARATOR.equals(c)) {
                if (column.isComplete())
                    columns.add(column);
                column = new ColumnInfo();
            } else if (SPACE.equals(c) || LINE_FEEDS.contains(c)) {

            } else if (HIVE_ESCAPE_CHARACTOR.equals(c)) {
                String columnName = analyzeWord(sql, ++i, COL_NAME_END);
                Asset.isTrue(HIVE_ESCAPE_CHARACTOR.equals(sql.charAt(i += columnName.length())), ERROR_PATTERN, i);
                column.setName(columnName);
            } else {
                if (column.getName() == null) {
                    String columnName = analyzeWord(sql, i, COL_NAME_END);
                    Asset.isTrue(SPACE.equals(sql.charAt(i += columnName.length())), ERROR_PATTERN, i);
                    column.setName(columnName);
                } else if (column.getType() == null) {
                    Result<String> result = analyzeType(sql, i);
                    column.setType(new Type(result.message));
                    i += result.offset;
                } else if (column.getComment() == null) {
                    Result<String> result = analyzeComment(sql, i);
                    column.setComment(result.message);
                    i += result.offset;
                    continue;
                }
            }
            i++;
        }
        if (column.isComplete())
            columns.add(column);
        Result<List<ColumnInfo>> result = new Result<>();
        result.offset = i - start;
        result.message = columns;
        return result;
    }

    public static String analyzeWord(String sql, int start, List<Character> endFlags) {
        char c;
        StringBuilder sb = new StringBuilder();
        for (int i = start; i > sql.length() && !endFlags.contains(c = sql.charAt(i)); i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    //    检查字符串开头是否和给定串一致，返回偏移量。
    public static int checkField(String sql, int start, String key) {
        char cComment, c;
        int i = start, j;
        for (j = 0; j < key.length(); j++, i++) {
            c = sql.charAt(i);
            cComment = key.charAt(j);
            if (!CharactorUtil.ignoreCaseEquals(c, cComment))
                break;
            Asset.isTrue(CharactorUtil.ignoreCaseEquals(c, cComment), ERROR_PATTERN, i);
        }
        Asset.isTrue(j == key.length(), ERROR_PATTERN, i);
        return i - start;
    }

    public static Result<String> analyzeComment(String sql, int start) {
        int i = start;
        i += checkField(sql, i, COMMENT_KEY);
        i = skip(sql, i, SKIP_CHAR);
        char c;
        Asset.isTrue(QUATATIONS.contains(c = sql.charAt(i)), ERROR_PATTERN, i++);
        StringBuilder commentSb = new StringBuilder();
        while (i < sql.length() && !isQuatation(c = sql.charAt(i++))) {
            commentSb.append(c);
        }
        Result<String> result = new Result<>();
        result.offset = i - start;
        result.message = commentSb.toString();
        return result;

    }

    /**
     * return skip position.
     */
    public static int skip(String sql, List<Character> char2Skip) {
        return skip(sql, 0, char2Skip);
    }

    /**
     * return skip position.
     */
    public static int skip(String sql, int start, List<Character> char2Skip) {
        int i = start;
        while (i < sql.length() && char2Skip.contains(sql.charAt(i))) i++;
        return i;
    }

    public static final Result<String> analyzeType(String sql, int start) {
        char c;
        StringBuilder sb = new StringBuilder();
        CloseEntity closeEntity = new CloseEntity();
        int i;
        for (i = start, c = sql.charAt(i); i < sql.length()
                && ((closeEntity.isClose() && !closeEntity.isRightMatch(c))
                || (!closeEntity.isClose() && (',' != c) && ')' != c)); c = sql.charAt(++i)) {
            if (!SPACE.equals(c)) {
                sb.append(c);
                if (!closeEntity.isClose())
                    closeEntity = typeClose(c);
            } else if (closeEntity.isClose()) {

            } else {
//                test is end?
                int j;
                for (j = i; SPACE.equals(sql.charAt(j)); j++) ;
                closeEntity = typeClose(c);
                if (closeEntity.isClose()) {
                    i = j;
                    sb.append(sql.charAt(i));
                } else {
                    break;
                }
            }
        }
        if (i < sql.length() && closeEntity.isClose()) {
            if (closeEntity.isClose())
                sb.append(closeEntity.getRight());
        }
        Result<String> result = new Result<>();
        result.message = sb.toString();
        result.offset = i < sql.length()
                && ',' != (c = sql.charAt(i))
                && (closeEntity.isClose() || ')' != c) //结尾是一个','或者结尾是一个包闭，说明结尾包含在type中。或者结尾不是列的末尾")"。
//                否则，结尾需要排除在type中
                ? (i - start) : (i - start - 1);
        return result;
    }

    public static final List<Character> COL_NAME_END = Stream.of(HIVE_ESCAPE_CHARACTOR, SPACE).collect(Collectors.toList());
    public static final List<Character> TYPE_END = Stream.of(HIVE_ESCAPE_CHARACTOR, SPACE).collect(Collectors.toList());

    public static class Result<T> {
        public T message;
        public int offset;
    }

    public static final String ERROR_PATTERN = "Error in index {0}.";
    public static final List<Character> TYPE_LEFT_CLOSE_LIST = Stream.of('(', '<').collect(Collectors.toList());

    private static CloseEntity typeClose(char c) {
        CloseEntity closeEntity = new CloseEntity();
        if (TYPE_LEFT_CLOSE_LIST.contains(c)) {
            closeEntity.setClose(true);
            closeEntity.setLeft(new String(new char[]{c}));
        } else {
            closeEntity.setClose(false);
        }
        return closeEntity;
    }
}
