package cn.com.sherhom.db.tool.utils;

/**
 * @author Sherhom
 * @date 2021/4/12 9:46
 */
public class CharactorUtil {
    public static boolean ignoreCaseEquals(char c1,char c2){
        return c1==c2||c1==c2+32||c2==c1+32;
    }
}
