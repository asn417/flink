package utils;

import com.google.common.hash.Hashing;
import org.apache.commons.lang.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * @Author: wangsen
 * @Date: 2020/4/2 17:53
 * @Description:
 **/
public class MD5Utils {

    /**
     * 获取MD5哈希
     * @author Leo
     * @date:Apr 5, 2016 11:29:04 AM
     * @param encData   需要哈希的字符
     * @return
     */
    @Deprecated
    public static String getMD5Code(String encData){
        if (StringUtils.isEmpty(encData)) {
            return null;
        }
        return hash(encData);
    }

    /**
     * md5 哈希
     * 推荐此方法，不会返回null
     * @param input 原文，不允许为null
     * @return
     */
    public static String hash(String input) {
        return Hashing.md5().hashString(Objects.requireNonNull(input, "input cannot be null!"), StandardCharsets.UTF_8).toString();
    }

    /**
     * 验证是否是MD5哈希
     * @author Leo
     * @date:Apr 5, 2016 11:29:52 AM
     * @param encData			需要比对的原文
     * @param encryptStr		需要比对的哈希值
     * @return
     */
    public static boolean validateMD5Code(String encData,String encryptStr) {
        if (StringUtils.isEmpty(encData) || StringUtils.isEmpty(encryptStr) ) {
            return false;
        }
        return hash(encData).equalsIgnoreCase(encryptStr);
    }

    public static void main(String[] args) {
        String str = getMD5Code("123456" + "wojiacloud");
        String str2 = Hashing.md5().hashString("123456wojiacloud", StandardCharsets.UTF_8).toString();
        String str3 = crypt("123456wojiacloud");
        System.out.println(str);
        System.out.println(str2);
        System.out.println(str3);
        System.out.println(Objects.equals(str, str2));
        System.out.println(Objects.equals(str, str3));
    }

    /**
     * MD5哈希
     * @author Tony
     * @date:Dec 3, 2016 5:19:24 PM
     * @param str
     * @return
     */
    @Deprecated
    public static String crypt(String str) {
        return getMD5Code(str);
    }
}
