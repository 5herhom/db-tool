package cn.com.sherhom.db.tool.test;

import cn.com.sherhom.db.tool.constant.Engine;
import cn.com.sherhom.db.tool.entity.waterdrop.WaterDropConfig;
import cn.com.sherhom.db.tool.entity.waterdrop.WdArgs;
import cn.com.sherhom.db.tool.utils.ClickhouseSqlAnalyzer;
import cn.com.sherhom.db.tool.utils.FileUtil;
import cn.com.sherhom.db.tool.utils.HiveSqlAnalyzer;
import cn.com.sherhom.db.tool.utils.SqlReadUtil;
import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author Sherhom
 * @date 2021/3/22 11:19
 */
public class ToolTest {
    @Test
    public void toolTest(){
        String table="db01.table01";
        String file= FileUtil.getSqlFile(table);
        List<String> sqls= SqlReadUtil.readSql(file);
        sqls.forEach(sql->{
            HiveSqlAnalyzer hiveSqlAnalyzer=new HiveSqlAnalyzer();
            hiveSqlAnalyzer.loadFromCreateSql(sql);
            System.out.println(JSON.toJSONString(hiveSqlAnalyzer.columnInfos()));
        });

    }

    @Test
    public void hive2Ck(){
        String table="db01.table01";
        String file= FileUtil.getSqlFile(table);
        List<String> sqls= SqlReadUtil.readSql(file);
        sqls.forEach(sql->{
            HiveSqlAnalyzer hiveSqlAnalyzer=new HiveSqlAnalyzer();
            hiveSqlAnalyzer.loadFromCreateSql(sql);
            ClickhouseSqlAnalyzer clickhouseSqlAnalyzer=new ClickhouseSqlAnalyzer();
            clickhouseSqlAnalyzer.loadByAnalyzer(hiveSqlAnalyzer);
            clickhouseSqlAnalyzer.setZkBasePath("/clickhouse-server/tables/");
            clickhouseSqlAnalyzer.setCluster("cluster_shard2");
            clickhouseSqlAnalyzer.setOrderbyKey("y,m,d");
//            clickhouseSqlAnalyzer.addExcludeColumn("mappp");
            System.out.println(clickhouseSqlAnalyzer.generateCreateTableSql(Engine.REPLICATED_MERGETREE));
            System.out.println(clickhouseSqlAnalyzer.generateCreateTableSql(Engine.DISTRIBUTED));
        });
    }
    @Test
    public void wdTest01(){

        String table="db01.table01";
        String file= FileUtil.getSqlFile(table);
        List<String> sqls= SqlReadUtil.readSql(file);
        sqls.forEach(sql->{
            HiveSqlAnalyzer hiveSqlAnalyzer=new HiveSqlAnalyzer();
            hiveSqlAnalyzer.loadFromCreateSql(sql);
            ClickhouseSqlAnalyzer clickhouseSqlAnalyzer=new ClickhouseSqlAnalyzer();
            clickhouseSqlAnalyzer.loadByAnalyzer(hiveSqlAnalyzer);
            clickhouseSqlAnalyzer.setZkBasePath("/clickhouse-server/tables/");
            clickhouseSqlAnalyzer.setCluster("cluster_shard2");
            clickhouseSqlAnalyzer.setOrderbyKey("name01");
            clickhouseSqlAnalyzer.setShardKey("id");
            clickhouseSqlAnalyzer.setShardNum(2);
//            clickhouseSqlAnalyzer.addExcludeColumn("mappp");
//            System.out.println(clickhouseSqlAnalyzer.generateCreateTableSql(Engine.REPLICATED_MERGETREE));
//            System.out.println(clickhouseSqlAnalyzer.generateCreateTableSql(Engine.DISTRIBUTED));
            WdArgs wdArgs=new WdArgs();
            wdArgs.setClickhouseSqlAnalyzer(clickhouseSqlAnalyzer);
            wdArgs.setHiveSqlAnalyzer(hiveSqlAnalyzer);
            wdArgs.setHost("127.0.0.1:8123");
            wdArgs.setShardNo(0);
            wdArgs.setUsername("sherhom");
            wdArgs.setPassword("passwwwwwddd");
            wdArgs.setPreWhere("name01=\"asda\" and name02=\"hzy\"");
            WaterDropConfig config=new WaterDropConfig(wdArgs);
            System.out.println(config.toConfString());
        });
    }
}
