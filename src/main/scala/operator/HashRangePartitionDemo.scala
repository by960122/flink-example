package operator

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description:分区
 */
object HashRangePartitionDemo {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: ListBuffer[(Int, String)] = ListBuffer[Tuple2[Int, String]]()
    data.append((1, "hello1"))
    data.append((2, "hello2"))
    data.append((2, "hello3"))
    data.append((3, "hello4"))
    data.append((3, "hello5"))
    data.append((3, "hello6"))
    data.append((4, "hello7"))
    data.append((4, "hello8"))
    data.append((4, "hello9"))
    data.append((4, "hello10"))
    data.append((5, "hello11"))
    data.append((5, "hello12"))
    data.append((5, "hello13"))
    data.append((5, "hello14"))
    data.append((5, "hello15"))
    data.append((6, "hello16"))
    data.append((6, "hello17"))
    data.append((6, "hello18"))
    data.append((6, "hello19"))
    data.append((6, "hello20"))
    data.append((6, "hello21"))
    val text: DataSet[(Int, String)] = env.fromCollection(data)
    println("Hash 分区: ")
    text.partitionByHash(0).mapPartition(it => {
      while (it.hasNext) {
        val tu = it.next()
        println("当前线程id: " + Thread.currentThread().getId + "," + tu)
      }
      it
    }).print()

    //    println("Range 分区: ")
    //    text.partitionByRange(0).mapPartition(it => {
    //      while (it.hasNext) {
    //        val tu = it.next()
    //        println("当前线程id: " + Thread.currentThread().getId + "," + tu)
    //      }
    //      it
    //    }).print()
  }
}
