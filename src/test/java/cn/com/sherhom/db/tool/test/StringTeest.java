package cn.com.sherhom.db.tool.test;

import org.junit.jupiter.api.Test;

/**
 * @author Sherhom
 * @date 2021/4/12 18:22
 */
public class StringTeest {
    @Test
    public void replaTest(){
        String s="{0} 123123";
        String r="hzy";
        System.out.println(s.replace("{0}",r));
    }
}
