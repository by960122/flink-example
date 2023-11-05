package source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import source.custorm.NoParalleSource
import org.apache.flink.api.scala._

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 使用没有并行度的source
 */
object NoParalleSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    添加自定义的source组件
    val text: DataStream[Long] = env.addSource(new NoParalleSource)
    val mapData: DataStream[Long] = text.map(line => {
      println("接收到的数据：" + line)
      line
    })
//    每隔2秒进行一次求和操作
    val sum: DataStream[Long] = mapData.timeWindowAll(Time.seconds(2)).sum(0)
    sum.print().setParallelism(1)
    env.execute("StreamingDemoWithMyNoParalleSourceScala")
  }
}
