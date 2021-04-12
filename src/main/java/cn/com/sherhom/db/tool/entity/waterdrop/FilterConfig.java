package cn.com.sherhom.db.tool.entity.waterdrop;

import cn.com.sherhom.db.tool.entity.ColumnInfo;
import cn.com.sherhom.db.tool.utils.HiveSqlAnalyzer;
import cn.com.sherhom.db.tool.utils.SqlAnalyzer;
import com.alibaba.fastjson.JSON;

import java.util.*;

/**
 * @author Sherhom
 * @date 2021/4/12 14:40
 */
public class FilterConfig {
    private List<FilterPlugin> plugins=new ArrayList<>();
    public void addPlugins(FilterPlugin filterPlugin){
        plugins.add(filterPlugin);
    }
    public  List<FilterPlugin>  plugins(){
        return this.plugins;
    }
    public String toConfString(){
        List<FilterPlugin> plugins=this.plugins;
        Map<String,Object> tmpConf=new HashMap<>();
        String tmpKey;
        FilterPlugin plugin;
        Map<String,Object> tmpPlugin;
        for(int i=0;i<plugins.size();i++){
            plugin=plugins.get(i);
            tmpKey=getTmpKey(i);
            tmpPlugin=new HashMap<>();
            for (Map.Entry<Object,Object> e:plugin.entrySet()
                 ) {
                tmpPlugin.put(e.getKey().toString(),e.getValue());
            }
            tmpConf.put(tmpKey,tmpPlugin);
        }
        String jsonString= JSON.toJSONString(tmpConf,true);
        for(int i=0;i<plugins.size();i++){
            plugin=plugins.get(i);
            tmpKey=getTmpKey(i);
            jsonString=jsonString.replace(tmpKey,plugin.type.name);
        }
        return jsonString;
    }
    public void loadPluginFromHive(HiveSqlAnalyzer hiveSqlAnalyzer,Set<String> validColNames){
        List<ColumnInfo> columnInfos=hiveSqlAnalyzer.columnInfos();
        String typeName;
        for (ColumnInfo columnInfo:columnInfos){
            if(validColNames!=null&&!validColNames.contains(columnInfo.getName().toLowerCase()))
                continue;
            typeName=columnInfo.getType().getName();
            if(SqlAnalyzer.typeEquals(typeName,"timestamp")){
                addPlugins(FilterPlugin.newConvert(columnInfo.getName(),"string"));
            }
            else if(typeName.trim().toUpperCase().startsWith("MAP")){
                addPlugins(FilterPlugin.newConvert(columnInfo.getName(),"string"));
            }
        }
    }
    public String getTmpKey(int i){
        return "{"+i+"}";
    }
    public String getTmpKeyForReplace(int i){
        return getTmpKey(i).replace("{","\\{").replace("}","\\}");
    }

    public static class FilterPlugin extends Properties {
        FilterType type;
//        convert keys:
        public final static String SOURCE_FILED_KEY="source_field";
        public final static String NEW_TYPE="new_type";
        public static FilterPlugin newConvert(String sourceField,String newType){
            FilterPlugin filterPlugin=new FilterPlugin();
            filterPlugin.type=FilterType.CONVERT;
            filterPlugin.put(SOURCE_FILED_KEY,sourceField);
            filterPlugin.put(NEW_TYPE,newType);
            return filterPlugin;
        }
    }
    public enum FilterType{
        CONVERT("convert");
        String name;
        FilterType(String name){
            this.name=name;
        }
    }
}
