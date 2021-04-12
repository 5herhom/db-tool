package cn.com.sherhom.db.tool.utils;

import cn.com.sherhom.db.tool.constant.Engine;
import cn.com.sherhom.db.tool.entity.ColumnInfo;
import cn.com.sherhom.db.tool.entity.Type;
import cn.com.sherhom.reno.common.exception.RenoException;
import cn.com.sherhom.reno.common.utils.CollectionUtils;
import lombok.Data;

import java.util.List;
import java.util.Properties;

/**
 * @author Sherhom
 * @date 2021/4/12 10:28
 */
@Data
public class ClickhouseSqlAnalyzer extends AbstractSqlAnalyzer {
    String cluster;
    String orderbyKey;
    String zkBasePath;
    Properties settings = new Properties();

    @Override
    public void loadByAnalyzer(SqlAnalyzer analyzer) {
        super.loadByAnalyzer(analyzer);
        if (this.partitions != null)
            this.columnInfos.addAll(partitions);
        if (analyzer instanceof HiveSqlAnalyzer) {
            List<ColumnInfo> newColumns =
                    columnInfos.stream().map(oldCol -> {
                        ColumnInfo newCol = oldCol.copy();
                        newCol.setType(hiveType2CkType(oldCol.getType()));
                    })
        }
    }

    public String generateCreateTableSql(Engine engine){

    }
    public String generateCreateTableSql(Engine engine,Properties settings){
        String database=this.database;
        String table=engine.tableName(this.table);
        String tableSql=database==null?table:database+"."+table;
        List<ColumnInfo>columnInfos=this.columnInfos;
        List<ColumnInfo> partitions=this.partitions;
        String tableComment=this.tableComment;
        String cluster=this.cluster;
        StringBuilder ssb= new StringBuilder();
        ssb.append(CREATE_HEAD).append(SPACE).append(CREATE_TABLE).append(SPACE).append(tableSql).append(SPACE);
        if(cluster!=null){
            ssb.append("on cluster ").append(cluster).append(SPACE);
        }
        ssb.append("(");
        columnInfos.forEach(columnInfo -> addColString(ssb,columnInfo));
        ssb.deleteCharAt(ssb.length()-1);
        ssb.append(")").append(SPACE).append(generateEngineSql(engine)).append(SPACE);
        if(engine!=Engine.DISTRIBUTED){
            if(!CollectionUtils.isEmpty(partitions)){
                ssb.append(CK_PARTITION_FIRST).append(SPACE).append(CK_PARTITION_SECOND).append(SPACE).append("(");
                partitions.forEach(partition->ssb.append(DOUBLE_QUATATIONS).append(partition.getName()).append(DOUBLE_QUATATIONS).append(","));
                ssb.deleteCharAt(ssb.length()-1).append(")").append(SPACE);
            }
            if(orderbyKey!=null){
                ssb.append(CK_ORDER_BY_FIRST).append(SPACE).append(CK_ORDER_BY_SECOND).append(SPACE).append("(").append(orderbyKey).append(")").append(SPACE);
            }
        }
        settings=CollectionUtils.isEmpty(settings)?this.settings:settings;
        if(!CollectionUtils.isEmpty(settings)){
            ssb.append(CK_SETTING_KEY).append(SPACE);
            settings.forEach((k,v)->ssb.append(k).append("=").append(v).append(SPACE));
        }
        ssb.append(SQL_ENDING);
        return ssb.toString();
    }
    public static final String CK_PARTITION_FIRST = "PARTITION";
    public static final String CK_PARTITION_SECOND = "BY";
    public static final String CK_ORDER_BY_FIRST = "ORDER";
    public static final String CK_ORDER_BY_SECOND = "BY";
    public static final String CK_SETTING_KEY = "SETTINGS";

    public Type hiveType2CkType(Type hiveType) {
        Type ckType = null;
        String ckTypeName = null;
        String[] ckArgs = null;
        String hiveTypeName = hiveType.getName();
        if (SqlAnalyzer.typeEquals(hiveTypeName, "string") || SqlAnalyzer.typeEquals(hiveTypeName, "VARCHAR") ||
                hiveTypeName.toLowerCase().trim().startsWith("map")) {
            ckTypeName = "String";
        } else if (SqlAnalyzer.typeEquals(hiveTypeName, "CHAR")) {
            ckTypeName = "Fixedstring";
            ckArgs = hiveType.getArgs();
        } else if (SqlAnalyzer.typeEquals(hiveTypeName, "timestamp")) {
            ckTypeName = "DateTime";
//            ckTypeName="DateTime64";
//            ckArgs=new String[1];
//            ckArgs[0]="3";
        } else if (SqlAnalyzer.typeEquals(hiveTypeName, "DATE")) {
            ckTypeName = "Date";
        } else if (SqlAnalyzer.typeEquals(hiveTypeName, "decimal")) {
            ckTypeName = "Decimal";
            ckArgs = hiveType.getArgs();
        } else if (SqlAnalyzer.typeEquals(hiveTypeName, "DOUBLE")) {
            ckTypeName = "Float64";
        } else if (SqlAnalyzer.typeEquals(hiveTypeName, "FLOAT")) {
            ckTypeName = "Float32";
        } else if (SqlAnalyzer.typeEquals(hiveTypeName, "BOOLEAN")
                || SqlAnalyzer.typeEquals(hiveTypeName, "TINYINT")) {
            ckTypeName = "UInt8";
        } else if (SqlAnalyzer.typeEquals(hiveTypeName, "SMALLINT")) {
            ckTypeName = "Int16";
        } else if (SqlAnalyzer.typeEquals(hiveTypeName, "INT")) {
            ckTypeName = "Int32";
        } else if (SqlAnalyzer.typeEquals(hiveTypeName, "BIGINT")) {
            ckTypeName = "Int64";
        } else {
            throw new RenoException("Unsupport type:" + hiveTypeName);
        }
        return new Type(ckTypeName, ckArgs);
    }

    public StringBuilder addColString(StringBuilder ssb, ColumnInfo columnInfo) {
        ssb.append(DOUBLE_QUATATIONS).append(columnInfo.getName()).append(DOUBLE_QUATATIONS).append(SPACE)
                .append(columnInfo.getType()).append(SPACE);
        if (columnInfo.getComment() != null)
            ssb.append(COMMENT_KEY).append(SPACE)
                    .append(SINGLE_QUATATIONS).append(columnInfo.getComment()).append(SINGLE_QUATATIONS).append(SPACE);
        ssb.append(COL_SEPARATOR);
        return ssb;
    }
    public String generateEngineSql(Engine engine){
        StringBuilder engineSb=new StringBuilder("ENGINE = ");
        if(engine==Engine.REPLICATED_MERGETREE){
            engineSb.append(engine.engineName).append("(")
                    .append(SINGLE_QUATATIONS).append(this.zkBasePath);
            if(engineSb.charAt(engineSb.length()-1)!='/')
                engineSb.append("/");
            engineSb.append("{shard}/{database}/{table}").append(SINGLE_QUATATIONS)
                    .append(",").append(SINGLE_QUATATIONS).append("{replica}").append(SINGLE_QUATATIONS)
                    .append(")");
        }
        else if (engine==Engine.DISTRIBUTED){
            engineSb.append(engine.engineName).append("(").append(SINGLE_QUATATIONS)
                    .append(this.cluster).append(SINGLE_QUATATIONS).append(",")
                    .append(SINGLE_QUATATIONS).append(this.database).append(SINGLE_QUATATIONS)
                    .append(",").append(SINGLE_QUATATIONS).append(Engine.REPLICATED_MERGETREE.tableName(this.table))
                    .append(SINGLE_QUATATIONS).append(",")
                    .append("rand()").append(")");
        }
        else{

        }
        return engineSb.toString();
    }
}
