package org.apache.kafka.my;

import org.junit.Test;

import java.util.ArrayDeque;

/**
 * 双端队列测试
 * @see
 * https://blog.csdn.net/u013309870/article/details/71478120
 *
 */
public class DequeTest {

    @Test
    public void testArrayQueue() {
        int []arr={1,2,3,4,5,6,7,8};
        ArrayDeque<Integer> aDeque=new ArrayDeque<Integer>();
        for(int i=0;i<arr.length;i++)
        {
            if((i&1)==0)
                //如果i是偶数从队头加入，否则从队尾加入
                aDeque.addFirst(arr[i]);
            else
                aDeque.addLast(arr[i]);

        }
        while(!aDeque.isEmpty())
        {
            aDeque.pollFirst();
            aDeque.pollLast();//此处不严谨但是由于arr的元素恰好是偶数个，本例中也不会出现问题。
        }
    }


}
