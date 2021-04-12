package cn.com.sherhom.db.tool.entity.waterdrop;

import cn.com.sherhom.db.tool.entity.ColumnInfo;
import cn.com.sherhom.db.tool.utils.ClickhouseSqlAnalyzer;
import cn.com.sherhom.db.tool.utils.HiveSqlAnalyzer;
import cn.com.sherhom.reno.common.utils.CollectionUtils;

import java.text.MessageFormat;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * @author Sherhom
 * @date 2021/4/12 14:39
 */
public class InputConfig {
    public static class HIVE {
        public String pre_sql;
        public String table_name ;
        private HiveSqlAnalyzer hiveSqlAnalyzer;

        public HIVE(HiveSqlAnalyzer hiveSqlAnalyzer) {
            this.table_name = "tmp_" + hiveSqlAnalyzer.table() + "_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().replace("-","_");
            this.hiveSqlAnalyzer = hiveSqlAnalyzer;
        }
        public static final String INPUT_SQL_PATTERN="select {0} from {1}.{2} where abs(hash({3}))={4,number,#} ";
        public void generatePreSql(ClickhouseSqlAnalyzer clickhouseSqlAnalyzer,int shardNo){
            generatePreSql(clickhouseSqlAnalyzer,null,shardNo);
        }
        public void generatePreSql(ClickhouseSqlAnalyzer clickhouseSqlAnalyzer,String preWhere,int shardNo) {
            String colStr = "*";
            HiveSqlAnalyzer hiveSqlAnalyzer=this.hiveSqlAnalyzer;
            List<ColumnInfo> validColumns = clickhouseSqlAnalyzer.getValidColumns();
            if (validColumns.size() != clickhouseSqlAnalyzer.columnInfos().size()) {
                StringJoiner stringJoiner = new StringJoiner(",");
                validColumns.forEach(validColumn->stringJoiner.add(validColumn.getName()));
                colStr=stringJoiner.toString();
            }
            this.pre_sql= MessageFormat.format(INPUT_SQL_PATTERN,colStr,hiveSqlAnalyzer.database(),
                    hiveSqlAnalyzer.table(),clickhouseSqlAnalyzer.getShardKey(),shardNo);
            if(preWhere!=null){
                this.pre_sql+=" and "+preWhere;
            }
        }
    }

    public InputConfig(HiveSqlAnalyzer hiveSqlAnalyzer) {
        this.hive = new HIVE(hiveSqlAnalyzer);
    }

    public HIVE hive;

}
