package cn.com.sherhom.db.tool.utils;

import cn.com.sherhom.db.tool.entity.waterdrop.WaterDropConfig;
import com.alibaba.fastjson.JSONObject;

/**
 * @author Sherhom
 * @date 2021/3/22 10:07
 */
public class WaterDropUtils {
    public static String jsonToWaterDropConfig(String jsonString){
        WaterDropConfig waterDropConfig= JSONObject.parseObject(jsonString,WaterDropConfig.class);
        System.out.println(JSONObject.toJSONString(waterDropConfig));
        return null;
    }
}
