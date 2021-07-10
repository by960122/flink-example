package operator

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description:外连接
 */
object OuterJoinDemo {
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

    println("左外连接: ")
    text1.leftOuterJoin(text2).where(0).equalTo(0).apply((first, second) => {
      if (second == null) {
        (first._1, first._2, "null")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()

    println("右外连接: ")
    text1.rightOuterJoin(text2).where(0).equalTo(0).apply((first, second) => {
      if (first == null) {
        (second._1, "null", second._2)
      } else {
        (first._1, first._2, second._2)
      }
    }).print()

    println("全外连接: ")
    text1.fullOuterJoin(text2).where(0).equalTo(0).apply((first, second) => {
      if (first == null) {
        (second._1, "null", second._2)
      } else if (second == null) {
        (first._1, first._2, "null")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()
  }
}
