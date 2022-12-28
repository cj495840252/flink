package org.kafka;

import java.util.Arrays;

public class ABC {
    public static void func(int[] nums,int target){
        Arrays.sort(nums);
        int left=0;
        int right=nums.length-1;
        while (left<right){
            if (nums[left]+nums[right]>target)right--;
            else if (nums[left]+nums[right]<target)left++;
            else {
                System.out.println(nums[left]);
                System.out.println(nums[right]);
                return;
            }
        }
    }
}
