import java.util.HashSet;
import java.util.Set;

/**
 * @Author: wangsen
 * @Date: 2020/6/6 19:09
 * @Description: 给定一个未排序的整数数组，找出最长连续序列的长度。
 * 要求算法的时间复杂度为 O(n)。
 * 示例:
 * 输入: [100, 4, 200, 1, 3, 2]
 * 输出: 4
 * 解释: 最长连续序列是 [1, 2, 3, 4]。它的长度为 4。
 **/
public class LongestConsecutive_128 {
    public static void main(String[] args) {
        int[] arr = {4,3,2,1,6,4,5};
        System.out.println(longestConsecutive2(arr));
    }
    /***
     * @Author: wangsen
     * @Description: 双层循环
     * @Date: 2020/6/6
     * @Param: [nums]
     * @Return: int
     **/
    public static int longestConsecutive1(int[] nums) {
        if (nums == null)
            return 0;
        int length = nums.length;
        Set<Integer> set = new HashSet<>();
        for (int i=0;i<length;i++){
            set.add(nums[i]);
        }
        int max = 0;
        for (int i = 0; i < length; i++) {
            int result = 0;
            for (int j = 0; j < length; j++) {
                if (set.contains(nums[i] + j)){
                    result++;
                }else {
                    break;
                }
            }
            max = max > result?max:result;
        }
        return max;
    }

    /***
     * @Author: wangsen
     * @Description: 优化:方法一是标准的O(N^2)复杂度，因为外层循环和内层循环都会从头到尾依次遍历。
     * 其实分析一下会发现，对于同一个有序的子序列，只需要当外层循环是子序列中最小的那个值才开始内层遍历即可。
     * 那么如何判断是否为子序列中最小的那个数呢？只需通过判断set中是否存在当前数-1后的数，如果存在则继续减，不存在则表明当前数已为所在子序列的最小值。
     * @Date: 2020/6/6
     * @Param: [nums]
     * @Return: int
     **/
    public static int longestConsecutive2(int[] nums) {
        if (nums == null)
            return 0;
        int length = nums.length;
        Set<Integer> set = new HashSet<>();

        for (int i=0;i<length;i++){
            set.add(nums[i]);
        }
        int max = 0;
        for (int num:set){
            if (set.contains(num-1)){
                continue;
            }else {
                int len = 0;
                for (int i=0;i<length;i++){
                    if (set.contains(num+i)){
                        len++;
                    }else {
                        break;
                    }
                }
                max = max > len?max:len;
            }
        }
        return max;
    }
}
