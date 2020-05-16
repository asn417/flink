import java.util.HashMap;

/**
 * @Author: wangsen
 * @Date: 2020/5/15 8:39
 * @Description: 给定一个整数数组和一个整数 k，你需要找到该数组中和为 k 的连续的子数组的个数。
 *
 * 示例 1 :
 *
 * 输入:nums = [1,1,1], k = 2
 * 输出: 2 , [1,1] 与 [1,1] 为两种不同的情况。
 * 说明 :
 *
 * 数组的长度为 [1, 20,000]。
 * 数组中元素的范围是 [-1000, 1000] ，且整数 k 的范围是 [-1e7, 1e7]。
 **/
public class SubarraySum_560 {
    public static void main(String[] args) {
        int[] nums = {28,54,7,-70,22,65,-6};
        System.out.println(subarraySum(nums,100));
    }
    public static int subarraySum(int[] nums, int k) {
        int count = 0, sum = 0;
        HashMap< Integer, Integer > map = new HashMap < > ();
        map.put(0, 1);
        for (int i = 0; i < nums.length; i++) {
            sum += nums[i];
            if (map.containsKey(sum - k))
                count += map.get(sum - k);
            map.put(sum, map.getOrDefault(sum, 0) + 1);
        }
        return count;
    }
    /***
     * @Author: wangsen
     * @Description: 双重循环，暴力计算。
     * @Date: 2020/5/15
     * @Param: [nums, k]
     * @Return: int
     **/
    public static int subarraySum1(int[] nums, int k) {
        int result = 0;
        int sum = 0;

        for (int i = 0; i < nums.length;i++) {
            for (int j = i;j < nums.length;j++){
                sum += nums[j];
                if (sum == k){
                    result++;
                    continue;
                }
            }
            sum = 0;
        }
        return result;
    }
}
