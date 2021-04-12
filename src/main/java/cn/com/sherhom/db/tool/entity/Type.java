package cn.com.sherhom.db.tool.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.StringJoiner;

/**
 * @author Sherhom
 * @date 2021/4/9 10:02
 */
@Data
@NoArgsConstructor
public class Type {
    private String name;
    private String[] args;
    public Type(String typeString){
        typeString=typeString.trim();
        int indexOfArgStart=typeString.indexOf("(");
        if(indexOfArgStart==-1||typeString.charAt(typeString.length()-1)!=')'){
            this.name=typeString;
            this.args=new String[0];
            return;
        }
        String name=typeString.substring(0,indexOfArgStart);
        this.name=name;
        this.args=typeString.substring(indexOfArgStart+1,typeString.length()-1).split(",");
    }
    public Type(String name,String[] args){
        this.name=name;
        this.args=args;
    }

    @Override
    public String toString(){
        if(args==null||args.length==0)
            return this.name;
        StringJoiner stringJoiner=new StringJoiner(",",this.name+"(",")");
        for(String arg:args){
            stringJoiner.add(arg);
        }
        return stringJoiner.toString();
    }
}
