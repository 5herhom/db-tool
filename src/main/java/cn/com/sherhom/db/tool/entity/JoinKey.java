package cn.com.sherhom.db.tool.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Sherhom
 * @date 2021/4/9 14:39
 */
@Data
@AllArgsConstructor
public class JoinKey {
    String mainKey;
    String dimKey;
}
