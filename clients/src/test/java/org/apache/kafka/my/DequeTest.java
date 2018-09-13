package org.apache.kafka.my;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;

/**
 * 双端队列测试
 * @see
 * https://blog.csdn.net/u013309870/article/details/71478120
 * https://www.cnblogs.com/wxd0108/p/7366234.html
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

    @Test
    public void testPeekLast() {
        ArrayDeque<Integer> aDeque = new ArrayDeque<Integer>();
        aDeque.addFirst(1);

        Assert.assertTrue(aDeque.peekFirst()==1);
        Assert.assertTrue(aDeque.peekLast()==1);
    }

    /**
     * ArrayDeuqe里面有的elements[length],每次调用addLast方法，在elements开始位置添加元素
     */
    @Test
    public void testAddLast() {
        int n = 2;
        //双端队列长度必须是2的n次方
        int length = 2<<(n-1);

        int tail = length;
        Assert.assertEquals(tail = (length-1) & (tail+1),1);
        Assert.assertEquals(tail = (length-1) & (tail+1),2); // 011(3) & 110(6) = 010(2)
    }

    /**
     ArrayDeuqe里面有的elements[length],每次调用addFirst方法，在elements末尾位置添加元素
     */
    @Test
    public void testAddFirst() {
        int n = 2;
        //双端队列长度必须是2的n次方
        int length = 2<<(n-1);

        int head = 0;
        Assert.assertEquals(head = (head - 1) & (length - 1),3);
        Assert.assertEquals(head = (head - 1) & (length - 1),2);
    }
}
