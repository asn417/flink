import java.util.Map;
import java.util.TreeMap;

/**
 * @Author: wangsen
 * @Date: 2020/5/21 12:44
 * @Description: 给定一个字符串 s，找到 s 中最长的回文子串。你可以假设 s 的最大长度为 1000。
 * 示例 1：
 * 输入: "babad"
 * 输出: "bab"
 * 注意: "aba" 也是一个有效答案。
 * 示例 2：
 * 输入: "cbbd"
 * 输出: "bb"
 **/
public class LongestPalindrome {
    public static void main(String[] args) {
        System.out.println(longestPalindrome1("ccd"));
    }
    public static String longestPalindrome(String s) {
        if (s.equals("") || s == null)
            return s;
        TreeMap<Integer,String> map = new TreeMap<>();
        boolean b = help(s, 0, s.length() - 1, map);
        String res = null;
        if (b){
            Map.Entry<Integer, String> entry = map.lastEntry();
            System.out.println(entry.getKey());
            res = entry.getValue();
        }
        return res;
    }
    public static boolean help(String s, int l, int r, TreeMap<Integer,String> map){
        if (l>r && s.charAt(l-1) == s.charAt(r+1)){
            return true;
        }
        if (l>r && s.charAt(l-1) != s.charAt(r+1)){
            return false;
        }
        if (s.charAt(l) != s.charAt(r)){
            if (help(s,l+1,r,map)){
                map.put(r - l ,s.substring(l+1,r+1));
                return true;
            }
            if (help(s,l,r-1,map)){
                map.put(r - l,s.substring(l,r));
                return true;
            }
        }else {
            if (help(s,l+1,r-1,map)){
                map.put(r - l + 1,s.substring(l,r+1));
                return true;
            }
        }
        return false;
    }
    public static String longestPalindrome1(String s) {
        if (s.equals("") || s == null)
            return s;
        int size = 0;
        String str = null;
        boolean b = help1(s, 0, s.length() - 1, size,str);
        return str;
    }
    public static boolean help1(String s, int l, int r, int size,String str){
        if (l>r && s.charAt(l-1) == s.charAt(r+1)){
            return true;
        }
        if (l>r && s.charAt(l-1) != s.charAt(r+1)){
            return false;
        }
        if (s.charAt(l) != s.charAt(r)){
            if (help1(s,l+1,r,size,str)){
                return true;
            }
            if (help1(s,l,r-1,size,str)){
                return true;
            }
        }else {
            if (help1(s,l+1,r-1,size,str)){
                if (size < r - l + 1){
                    size = r - l + 1;
                    str = s.substring(l,r+1);
                }
                return true;
            }
        }
        System.out.println("size:"+size+",str:"+str);
        return false;
    }
}
