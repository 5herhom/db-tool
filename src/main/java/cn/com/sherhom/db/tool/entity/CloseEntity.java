package cn.com.sherhom.db.tool.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sherhom
 * @date 2021/4/9 16:18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CloseEntity {
    boolean isClose;
    String left;
    String right;
    static Map<String,String> L_R_Map=new HashMap<String,String>(){
        {
            put("(",")");
            put("<",">");
        }
    };
    public String getRight(){
        return L_R_Map.get(this.left);
    }
    public boolean isRightMatch(char c){
        return isRightMatch(new String(new char[]{c}));
    }
    public boolean isRightMatch(String right){
        if(!isClose)
            return false;
        String left=this.left;
        if(left==null||right==null)
            return false;
        return right.equals(L_R_Map.get(left));
    }

}
