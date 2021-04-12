package cn.com.sherhom.db.tool.constant;

/**
 * @author Sherhom
 * @date 2021/4/9 16:28
 */
public enum Engine {
    REPLICATED_MERGETREE(1,"ReplicateMergeTree","local"),
    DISTRIBUTED(2,"Distributed","all"),
    MERGE_TREE(3,"MergeTree"),
    ;

    public int typeNo;
    public String engineName;
    public String tableNameSuffix="";
    static final String TABLE_SEPARATOR="_";

    Engine(int typeNo, String engineName, String tableNameSuffix) {
        this.typeNo = typeNo;
        this.engineName = engineName;
        this.tableNameSuffix = tableNameSuffix;
    }

    Engine(int typeNo, String engineName) {
        this.typeNo = typeNo;
        this.engineName = engineName;
    }
    public String tableName(String table){return table+TABLE_SEPARATOR+this.tableNameSuffix;}
}
