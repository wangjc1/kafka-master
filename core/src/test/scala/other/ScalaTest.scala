package other

import org.scalatest.junit.JUnitSuite

object ScalaTest {

  def main(args: Array[String]): Unit = {
    val sst = new SocketServerTest()
  }

  //测试主构造器和辅助构造器
  class SocketServerTest extends JUnitSuite {
     //辅助构造器
     def this(name:String){
       this()
     }
     println("写到这里的语句也会成主构造器的一部分，当执行主构造器的时候，这里的语句也会执行")
  }
}
