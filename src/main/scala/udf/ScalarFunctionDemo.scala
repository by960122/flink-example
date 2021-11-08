package udf

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, Table, Tumble}
import org.apache.flink.table.functions.ScalarFunction
import table.TimeAndWindowDemo.WordWithCount


/**
 * Author:BYDylan
 * Date:2021/11/1
 * Description:
 */
object ScalarFunctionDemo {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val text: DataStream[String] = env.socketTextStream("127.0.0.1", 8888, '\n')
    val windowCountDataStream: DataStream[WordWithCount] = text.flatMap(l => l.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
      .window(TumblingProcessingTimeWindows.of(Time.seconds(3))) // 滚动窗口
      //      .window(TumblingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1))) // 滑动事件窗口
      //      .window(EventTimeSessionWindows.withGap(Time.seconds(3))) // 会话窗口
      //            .sum("count")
      .reduce((a, b) => WordWithCount(a.word, a.count + b.count))

    val blinkStreamingSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamingSettings)
    val table: Table = tableEnv.fromDataStream(windowCountDataStream, $("word"), $("count"), $("pt").proctime())

    //    1. table api
    val hashCode = new HashCode(2)
    table.select($"id",hashCode("id"))

  }
}

class HashCode(factor: Int) extends ScalarFunction {

  def eval(s: String): Int = {
    s.hashCode * factor - 10000
  }
}