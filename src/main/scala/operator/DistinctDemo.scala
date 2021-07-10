package operator

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description: 对数据进行去重
 */
object DistinctDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: ListBuffer[String] = ListBuffer[String]()
    data.append("hello you")
    data.append("hello me")
    val text: DataSet[String] = env.fromCollection(data)
    val flatMapData: DataSet[String] = text.flatMap(line => {
      val words = line.split(" ")
      for (word <- words) {
        println("单词: " + word)
      }
      words
    })
    flatMapData.distinct().print()
  }
}
