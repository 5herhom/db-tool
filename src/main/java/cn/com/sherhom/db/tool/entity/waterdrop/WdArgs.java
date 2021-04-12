package cn.com.sherhom.db.tool.entity.waterdrop;

import cn.com.sherhom.db.tool.utils.ClickhouseSqlAnalyzer;
import cn.com.sherhom.db.tool.utils.HiveSqlAnalyzer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author Sherhom
 * @date 2021/4/12 17:31
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WdArgs {
    HiveSqlAnalyzer hiveSqlAnalyzer;
    ClickhouseSqlAnalyzer clickhouseSqlAnalyzer;
    int shardNo;
    String preWhere;
    String host;
    String username;
    String password;
}
