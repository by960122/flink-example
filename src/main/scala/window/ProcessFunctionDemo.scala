package window

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import source.SensorReading

/** *
 * Author:BYDylan
 * Date:2021/7/23
 * Description: 温度连续上升N秒则输出告警,这种需求用任何一种窗口都会有遗漏
 */
object ProcessFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketStream: DataStream[String] = env.socketTextStream("127.0.0.1", 8888, '\n')

    val dataStream: DataStream[SensorReading] = socketStream.flatMap(l => l.split("\\s"))
      .map(data => {
        val arr = data.split(",")
        new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .keyBy(_.id)
      .process(new TempIncreWarning(10000L))
    dataStream.setParallelism(1).print()
    env.execute("ProcessFunctionDemo")
  }
}

class TempIncreWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {
  //  定义一个状态: 保存上一个温度值进行比较,还要保存 注册定时器的时间戳用于删除
  private lazy val lastTimestampState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastTimestamp", classOf[Long]))
  private lazy val lastTemperatureState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemperature", classOf[Double]))


  override def processElement(value: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //    先取出状态
    val lastTimestamp: Long = lastTimestampState.value()
    val lastTemperature: Double = lastTemperatureState.value()

//    更新温度值
    lastTemperatureState.update(value.temperature)

    //    当前温度值和上一温度值进行比较
    if (value.temperature > lastTemperature && lastTimestamp == 0) {
      //      如果温度上升,且没有定时器,那么注册当前数据时间10s后的定时器
      val timeService: Long = context.timerService().currentProcessingTime() + interval
      context.timerService().registerEventTimeTimer(timeService)
      lastTimestampState.update(timeService)
    } else if (value.temperature < lastTemperature) {
      context.timerService().deleteEventTimeTimer(lastTimestamp)
      lastTimestampState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器 " + ctx.getCurrentKey + " 的温度连续" + interval / 1000 + "秒上升")
    lastTimestampState.clear()
  }
}

//功能测试
class MykeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {

  override def open(parameters: Configuration): Unit = {
    val myState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("myState", classOf[Int]))
  }

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    context.getCurrentKey
    context.timestamp()
    context.timerService().currentWatermark()
    //    注册定时器
    context.timerService().registerEventTimeTimer(context.timestamp() + 60000L)
  }

  //  定时器触发的时候执行这个方法
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    super.onTimer(timestamp, ctx, out)
  }
}
