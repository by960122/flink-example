package source

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import source.custorm.RichParalleSource

/**
 * Author:BYDylan
 * Date:2020/5/6
 * Description:使用支持多并行度的source
 */
object RichParalleSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[Long] = env.addSource(new RichParalleSource).setParallelism(2)
    val mapData: DataStream[Long] = text.map(line => {
      println("接收到的数据：" + line)
      line
    })
    val sum: DataStream[Long] = mapData.timeWindowAll(Time.seconds(2)).sum(0)
    sum.print().setParallelism(1)
    env.execute("StreamingDemoWithMyRichParalleSourceScala")
  }
}
