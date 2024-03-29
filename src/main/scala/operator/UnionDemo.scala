package operator

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import source.custorm.NoParalleSource

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 合并,不限制个数,但限制类型
 */
object UnionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text1: DataStream[Long] = env.addSource(new NoParalleSource)
    val text2: DataStream[Long] = env.addSource(new NoParalleSource)
    val unionall = text1.union(text2)
    val sum: DataStream[Long] = unionall.map(line => {
      println("接收到的数据：" + line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)
    sum.print().setParallelism(1)
    env.execute("StreamingDemoUnionScala")
  }
}
