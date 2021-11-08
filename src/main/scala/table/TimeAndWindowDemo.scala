package table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.api.{AnyWithOperations, EnvironmentSettings, FieldExpression, LiteralIntExpression, Table, Tumble}
import org.apache.flink.types.Row

/**
 * Author:BYDylan
 * Date:2020/5/4
 * Description:
 */
object TimeAndWindowDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val blinkStreamingSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamingSettings)

    val text: DataStream[String] = env.socketTextStream("127.0.0.1", 8888, '\n')
    val windowCountDataStream: DataStream[WordWithCount] = text.flatMap(l => l.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
      .window(TumblingProcessingTimeWindows.of(Time.seconds(3))) // 滚动窗口
      //      .window(TumblingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1))) // 滑动事件窗口
      //      .window(EventTimeSessionWindows.withGap(Time.seconds(3))) // 会话窗口
      //            .sum("count")
      .reduce((a, b) => WordWithCount(a.word, a.count + b.count))

    val table: Table = tableEnv.fromDataStream(windowCountDataStream, $("word"), $("count"), $("pt").proctime())

    //    1. table api
    val resultTable1: Table = table.window(Tumble over 5.second on $"rowtime" as "rw")
      .groupBy($"id", $"rw")
      .select($"id", $"id".count(), $"temperature".avg(), $"rw".end())

    //    2. sql
    val resultTable2: Table = tableEnv.sqlQuery(
      """
        |select id,count(id),avg(temperature) from sensor group by id,tumble(ts,interval '10' second)
        |""".stripMargin)

//    Over window: 统计每个second每条数据,与之前两行数据的平均温度


    table.printSchema()
    table.toAppendStream[Row].print()
    //  执行任务
    env.execute("SocketWindowCount")
  }

  case class WordWithCount(word: String, count: Long)

}
