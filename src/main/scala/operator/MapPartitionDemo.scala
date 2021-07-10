package operator

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description: 使用mappartition，每次处理一批数据
 */
object MapPartitionDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = ListBuffer[String]()
    data.append("hello you")
    data.append("hello me")
    val text = env.fromCollection(data)
    text.mapPartition(it => {
      //      创建数据库连接，建议把这块代码放到try-catch代码块中
      val res = ListBuffer[String]()
      while (it.hasNext) {
        val line = it.next()
        val words = line.split(" ")
        for (word <- words) {
          res.append(word)
        }
      }
      res
//      关闭连接
    }).print()
  }
}
