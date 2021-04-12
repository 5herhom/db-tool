package cn.com.sherhom.db.tool.entity.waterdrop;

import cn.com.sherhom.db.tool.utils.ClickhouseSqlAnalyzer;
import cn.com.sherhom.db.tool.utils.HiveSqlAnalyzer;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Sherhom
 * @date 2021/3/22 10:22
 */
public class WaterDropConfig {
    public SparkConfig spark;
    public InputConfig input;
    public FilterConfig filter = new FilterConfig();
    public OutputConfig output = new OutputConfig();

    public WaterDropConfig(WdArgs wdArgs) {
        HiveSqlAnalyzer hiveSqlAnalyzer = wdArgs.getHiveSqlAnalyzer();
        ClickhouseSqlAnalyzer clickhouseSqlAnalyzer = wdArgs.getClickhouseSqlAnalyzer();
        Set<String> validColName = clickhouseSqlAnalyzer.getValidColumnNames();
        int shardNam = wdArgs.getShardNo();
        String preWhere = wdArgs.getPreWhere();
        this.spark = new SparkConfig("WaterDrop_" + clickhouseSqlAnalyzer.table() + "_"
                + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().replace("-", "_"));
        this.input = new InputConfig(hiveSqlAnalyzer);
        this.input.hive.generatePreSql(clickhouseSqlAnalyzer, preWhere, shardNam);
        this.filter.loadPluginFromHive(hiveSqlAnalyzer, validColName);
        this.output.clickhouse = new OutputConfig.Clickhouse();
        this.output.clickhouse.database = clickhouseSqlAnalyzer.database();
        this.output.clickhouse.table = clickhouseSqlAnalyzer.table();
        this.output.clickhouse.fields = validColName.stream().collect(Collectors.toList());
        this.output.clickhouse.username = wdArgs.getUsername();
        this.output.clickhouse.password = wdArgs.getPassword();

    }

    public String toConfString() {
        WaterDropConfig config = this;
        JSONObject jsonObject = (JSONObject) JSON.toJSON(config);
        String tmpValue = "{##}";
        jsonObject.put("filter", tmpValue);
        String jsonString = JSONObject.toJSONString(jsonObject, true);
        jsonString = jsonString.replace("\"" + tmpValue + "\"", filter.toConfString().replace("\n","\n\t"));
        return jsonString;
    }
}
