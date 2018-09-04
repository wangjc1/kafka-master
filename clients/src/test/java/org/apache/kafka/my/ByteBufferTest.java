package org.apache.kafka.my;

import org.junit.Test;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * @author: wangjc
 * 2018/9/4
 */
public class ByteBufferTest {

    @Test(expected = BufferOverflowException.class)
    public void overflowTest(){
        //写入
        ByteBuffer buf = ByteBuffer.allocate(2);
        byte a = 'a';
        byte b = 'b';
        buf.put(a);
        buf.put(b);

        //总共分配了2个字节，上面已经写满了，再写就溢出了
        buf.put(a); //error:java.nio.BufferOverflowException
    }

    @Test
    public void rewriteTest(){
        //写入
        ByteBuffer buf = ByteBuffer.allocate(3);
        byte a = 'a';
        byte b = 'b';
        buf.put(a);
        buf.put(b);

        //buf.flip(); //报错：因为flip后limit = 2，重新写入后，最多只能写入两个字符
        buf.clear();  //并不会清除缓冲区数据，而是让指向缓冲区的指针回到最开始位置
        //buf.rewind(); //OK

        byte c = 'c';
        byte d = 'd';
        byte e = 'e';
        buf.put(c);
        buf.put(d);
        buf.put(e);

        assertEquals(buf.limit(),3);
        assertEquals(buf.position(),3);
    }

    @Test
    public void flipTest(){
        //写入
        ByteBuffer buf = ByteBuffer.allocate(3);
        byte a = 'a';
        byte b = 'b';
        buf.put(a);
        buf.put(b);

        //flip()的作用是读取刚刚写入长度的数据，即读取position=0到limit=2之间的数据
        buf.flip(); //limit = position = 2 ,position=0，capacity=3
        assertEquals((char)buf.get(),a);
        assertEquals((char)buf.get(),b);
    }

    /*
    mark和reset是一对搭档，使用时要一起使用
     */
    @Test
    public void markTest(){
        //写入
        ByteBuffer buf = ByteBuffer.allocate(3);
        byte a = 'a';
        byte b = 'b';
        buf.put(a);
        buf.put(b);

        //flip()的作用是读取刚刚写入长度的数据，即读取position=0到limit=2之间的数据
        buf.flip(); //limit = position = 2 ,position=0，capacity=3
        assertEquals((char)buf.get(),a);

        //在当前读取位置打个标志，这样当调用reset后,可以回到此位置重新读取
        buf.mark();//java.nio.HeapByteBuffer[pos=1 lim=2 cap=3]
        assertEquals((char)buf.get(),b);//java.nio.HeapByteBuffer[pos=2 lim=2 cap=3]

        //让position重新回到mark位置，即position = mark = 1
        buf.reset();//java.nio.HeapByteBuffer[pos=1 lim=2 cap=3]
        assertEquals((char)buf.get(),b);//java.nio.HeapByteBuffer[pos=2 lim=2 cap=3]

    }
}
