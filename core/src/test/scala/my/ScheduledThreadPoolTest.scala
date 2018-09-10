package my

import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import kafka.zookeeper.{ZooKeeperClient, ZooKeeperClientTimeoutException}
import org.junit.Assert.assertEquals
import org.junit.Test

class ScheduledThreadPoolTest {

  @Test
  def testInterrupt(): Unit = {
    val t = new Thread("interruptable-thread") {
      override def run(): Unit = {
        try {
          Thread.sleep(10000)
        }
        catch {
          case e:InterruptedException =>
            println("发生了中断")
            Thread.currentThread.interrupt()
        }
      }
    }
    t.start()
    Thread.sleep(1000)
    t.interrupt()
    Thread.sleep(Integer.MAX_VALUE)
  }

  @Test
  def testScheduledThreadPool(): Unit = {
    val scheduledThreadPool = Executors.newScheduledThreadPool(5)
    for (i <- 1 to 3) {
      val worker = new Task("task-" + i)
      // 只执行一次
      //          scheduledThreadPool.schedule(worker, 5, TimeUnit.SECONDS);
      // 周期性执行，每5秒执行一次
      scheduledThreadPool.scheduleAtFixedRate(worker, 0,5, TimeUnit.SECONDS)
    }

    Thread.sleep(10000)

    println("Shutting down executor...")
    // 关闭线程池
    scheduledThreadPool.shutdown()
    var isDone = false
    // 等待线程池终止
    do {
      isDone = scheduledThreadPool.awaitTermination(1, TimeUnit.DAYS)
      println("awaitTermination...")
    } while(!isDone)

    println("Finished all threads")

    // Thread.sleep(Integer.MAX_VALUE)
  }

  class Task(name:String) extends Runnable {
    override def run(): Unit = {
      println("name = " + name + ", startTime = " + new Date())
      try {
        Thread.sleep(1000)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
      println("name = " + name + ", endTime = " + new Date())
    }
  }
}
