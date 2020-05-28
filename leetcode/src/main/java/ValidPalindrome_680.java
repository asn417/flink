import java.util.Arrays;
import java.util.List;

/**
 * @Author: wangsen
 * @Date: 2020/5/19 12:11
 * @Description: 给定一个非空字符串 s，最多删除一个字符。判断是否能成为回文字符串。
 * 示例 1:
 * 输入: "aba"
 * 输出: True
 * 示例 2:
 * 输入: "abca"
 * 输出: True
 * 解释: 你可以删除c字符。
 * 注意:
 * 字符串只包含从 a-z 的小写字母。字符串的最大长度是50000。
 **/
public class ValidPalindrome_680 {
    public static void main(String[] args) {
        String s = "aba";
        System.out.println(validPalindrome(s,0,s.length()-1,1));
    }
    private static boolean validPalindrome(String s, int l, int r, int c) {
        if(l > r){
            return true;
        }
        if(s.charAt(l) == s.charAt(r)){
            return validPalindrome(s,l+1,r-1,c);
        }else{
            if(c == 0){
                return false;
            }
            return validPalindrome(s,l,r-1,c-1) || validPalindrome(s,l+1,r,c-1);
        }
    }
}
