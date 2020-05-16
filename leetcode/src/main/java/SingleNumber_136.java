/**
 * @Author: wangsen
 * @Date: 2020/5/14 8:39
 * @Description: 给定一个非空整数数组，除了某个元素只出现一次以外，其余每个元素均出现两次。找出那个只出现了一次的元素。
 * 说明：
 * 你的算法应该具有线性时间复杂度。 你可以不使用额外空间来实现吗？
 * 示例 1:
 * 输入: [2,2,1]
 * 输出: 1
 * 示例 2:
 * 输入: [4,1,2,1,2]
 * 输出: 4
 **/
public class SingleNumber_136 {
    public static void main(String[] args) {
        int[] nums = {1,1,4,4,2,5,5,3,2,6,7,6,3};
        System.out.println(singleNumber(nums));
    }
    /***
     * @Author: wangsen
     * @Description: 思路：采用亦或算法（两个数对应位值相同则为0，不同则为1）
     * @Date: 2020/5/14
     * @Param: [nums]
     * @Return: int
     **/
    public static int singleNumber(int[] nums) {
        int result = 0;
        for (int i = 0; i < nums.length; i++) {
            result ^= nums[i];
        }
        return result;
    }
}
