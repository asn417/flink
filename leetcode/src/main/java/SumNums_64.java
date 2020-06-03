/**
 * @Author: wangsen
 * @Date: 2020/6/2 8:38
 * @Description: 求 1+2+...+n ，要求不能使用乘除法、for、while、if、else、switch、case等关键字及条件判断语句（A?B:C）。
 * 示例 1：
 * 输入: n = 3
 * 输出: 6
 * 示例 2：
 * 输入: n = 9
 * 输出: 45
 * 限制：
 * 1 <= n <= 10000
 **/
public class SumNums_64 {
    public static void main(String[] args) {
        System.out.println(help1(9));
    }
    /***
     * @Author: wangsen
     * @Description: 如果不限制if的使用，则直接这样递归即可
     * @Date: 2020/6/2
     * @Param: [n]
     * @Return: int
     **/
    public static int help(int n){
        if (n == 0)
            return 0;
        else
            return n + help(n - 1);
    }

    public static int help1(int n){
        int sum = n;
        boolean b = n == 0 || (sum += help1(n-1))>0;
        return sum;
    }
}
