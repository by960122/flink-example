package window

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import source.SensorReading

/**
 * Author:BYDylan
 * Date:2021/4/22
 * Description:水位线 watermark
 */
object WatermarkDemo {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    设置间隔 watermark
    //    env.getConfig.setAutoWatermarkInterval(500L)
    val sockText: DataStream[String] = env.socketTextStream("127.0.0.1", 8888, '\n')
    val dataStream: DataStream[SensorReading] = sockText.map(data => {
      val arr = data.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
      //      .assignAscendingTimestamps(_.timestamp * 1000L) // 升序 watermark,有序的情况下,单位:毫秒数
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) { // 这里定义起始最大延迟
        //        提取时间戳
        override def extractTimestamp(t: SensorReading) = t.timestamp * 1000L
      })

    val laterData: OutputTag[Tuple3[String, Double, Long]] = new OutputTag[Tuple3[String, Double, Long]]("later")
    val resultDataStream: DataStream[(String, Double, Long)] = dataStream.map(data => (data.id, data.temperature, data.timestamp))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
      .allowedLateness(Time.minutes(1)) // 允许延迟
      .sideOutputLateData(laterData) // 漏掉的数据测流输出
      .reduce((currentResult, newData) => (currentResult._1, currentResult._2.min(newData._2), newData._3))

    //    测流打印
    resultDataStream.getSideOutput(laterData).print().setParallelism(1)
    env.execute("Watermark demo")
  }
}
