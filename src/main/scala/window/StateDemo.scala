package window

import operator.custorm.SensorReadingReduce
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import source.SensorReading

/**
 * Author:BYDylan
 * Date:2021/4/23
 * Description: 键控状态
 */
object StateDemo {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sockText: DataStream[String] = env.socketTextStream("127.0.0.1", 8888, '\n')
    val dataStream: DataStream[SensorReading] = sockText.map(data => {
      val arr = data.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
    env.execute("State demo")
  }
}

//必须定义在 RichFunction 中,因为需要运行时上下文
class MyRichMapper extends RichMapFunction[SensorReading, String] {
  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState[Int](new ListStateDescriptor[Int]("listState", classOf[Int]))
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState[String, Double](new MapStateDescriptor[String, Double]("mapState", classOf[String], classOf[Double]))
  lazy val reducingState: ReducingState[SensorReading] = getRuntimeContext.getReducingState[SensorReading](new ReducingStateDescriptor[SensorReading]("reducingState", new SensorReadingReduce, classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
  }

  override def map(value: SensorReading): String = {
    val currentValue: Double = valueState.value()
    valueState.update(value.temperature)
    listState.add(1)
    mapState.put("1", 1)
    value.id
  }
}
