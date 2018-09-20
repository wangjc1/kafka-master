package org.apache.kafka.my;

import org.junit.Test;

/**
 *
 */
public class ThreadInterruptTest {

    public static void main(String[] args) {
        try {
            Thread t1 = new MyThread("t1");  // 新建“线程t1”
            System.out.println(t1.getName() + " (" + t1.getState() + ") is new.");

            t1.start();                      // 启动“线程t1”
            System.out.println(t1.getName() + " (" + t1.getState() + ") is started.");

            // 主线程休眠300ms，然后主线程给t1发“中断”指令。
            Thread.sleep(300);
            t1.interrupt();
            System.out.println(t1.getName() + " (" + t1.getState() + ",中断后抛异常前状态：" + t1.isInterrupted() + ") is interrupted.");

            // 主线程休眠300ms，然后查看t1的状态。
            Thread.sleep(300);
            System.out.println(t1.getName() + " (" + t1.getState() + ") is interrupted now.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void waitInteruptTest() throws Exception {
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t1.start();

        Thread.sleep(2000);
        t1.interrupt();

        t1.join();
    }

    private static class MyThread extends Thread {

        public MyThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            int i = 0;
            while (!isInterrupted()) {
                try {
                    Thread.sleep(100); // 休眠100ms
                } catch (InterruptedException ie) {
                    System.out.println(Thread.currentThread().getName() + " (" + this.getState() + ",中断后抛异常后状态：" + isInterrupted() + ") catch InterruptedException.");
                    //再次调用interrupt()恢复中断，因为抛出异常后会重置为未中断状态，所以如果想跳出循环，就再调用interrupt()让中断复位
                    //Thread.currentThread().interrupt();
                }
                i++;
                System.out.println(Thread.currentThread().getName() + " (" + this.getState() + ") loop " + i);
            }
        }
    }

}

