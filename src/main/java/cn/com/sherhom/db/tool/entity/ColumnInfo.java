package cn.com.sherhom.db.tool.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Sherhom
 * @date 2021/4/9 10:01
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColumnInfo {
    String name;
    Type type;
    String comment;
    public ColumnInfo(String str){
        String[] arr=str.split(" ");
        this.name=arr[0];
        this.type=new Type(arr[1]);
    }
    public boolean isComplete(){
        return StringUtils.isNotBlank(this.name)&&this.type!=null;
    }
    public ColumnInfo copy(){
        ColumnInfo columnInfo=new ColumnInfo();
        columnInfo.name=this.name;
        columnInfo.type=this.type==null?null:new Type(this.type.toString());
        columnInfo.comment=this.comment;
        return columnInfo;
    }
}
