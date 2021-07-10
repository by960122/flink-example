package operator

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description: inner join操作
 */
object InnerJoinDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data1 = ListBuffer[Tuple2[Int, String]]()
    data1.append((1, "zs"))
    data1.append((2, "ls"))
    data1.append((3, "ww"))
    val data2: ListBuffer[(Int, String)] = ListBuffer[Tuple2[Int, String]]()
    data2.append((1, "beijing"))
    data2.append((2, "shanghai"))
    data2.append((3, "guangzhou"))
    val text1: DataSet[(Int, String)] = env.fromCollection(data1)
    val text2: DataSet[(Int, String)] = env.fromCollection(data2)
    text1.join(text2).where(0).equalTo(0).apply((first, second) => {
      (first._1, first._2, second._2)
    }).print()
  }
}
