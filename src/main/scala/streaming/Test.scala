package streaming

/**
 * Author:BYDylan
 * Date:2020/5/8
 * Description:
 */
object Test {
  def main(args: Array[String]) {
    new Demo("aa").demo();
  }

  class Demo(val par: String) {
    def demo(): Unit = {
      println(par);
    }
  }

}

