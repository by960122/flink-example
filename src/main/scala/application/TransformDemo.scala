package application

import java.io.File
import operator.custorm.SensorReadingReduce
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.SensorReading


/**
 * Author:BYDylan
 * Date:2020/11/6
 * Description: 算子 操作
 */

object TransformDemo {
  private val projectPath: String = System.getProperty("user.dir")
  System.getProperty("log.file", "")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 0.读取数据
    val filePath = projectPath + File.separator + "doc\\sensor.txt"
    val fileStream: DataStream[String] = env.readTextFile(filePath)
    // 1.先转换成样例类类型（简单转换操作）
    val dataStream: DataStream[SensorReading] = fileStream.map(data => {
      val arr = data.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })


    // 2.根据id进行分组,输出每个传感器当前最小值,必须要是样例类才可以直接写字段
    val keyByStream: KeyedStream[SensorReading, String] = dataStream.keyBy(value => value.id)
    val minByStream: DataStream[SensorReading] = keyByStream.minBy("temperature")

    // 3.需要输出当前最小的温度值,以及最近的时间戳,要用reduce
    val resultStream = dataStream.keyBy("id").reduce(new SensorReadingReduce)

    // 4. 多流转换操作
    // 4.1 分流,将传感器温度数据分成低温、高温两条流
    //    flink 1.12 没得了
    //    val splitStream = dataStream
    //      .split(data => {
    //        if (data.temperature > 30.0) Seq("high") else Seq("low")
    //      })
    //    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    //    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    //    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")

    val highTag: OutputTag[SensorReading] = new OutputTag[SensorReading]("high")
    val lowTag: OutputTag[SensorReading] = new OutputTag[SensorReading]("low")
    val splitStream: DataStream[SensorReading] = dataStream.process(new ProcessFunction[SensorReading, SensorReading] {
      override def processElement(data: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
        if (data.temperature > 30.0) context.output(highTag, data)
        else context.output(lowTag, data)
      }
    })
    val highTempStream: DataStream[SensorReading] = splitStream.getSideOutput(highTag)
    val lowTempStream: DataStream[SensorReading] = splitStream.getSideOutput(lowTag)
    highTempStream.print("high")
    lowTempStream.print("low")

    // 4.2 合流,connect
    val warningStream: DataStream[(String, Double)] = highTempStream.map(data => (data.id, data.temperature))
    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)

    // 用coMap对数据进行分别处理,注意看前面返回类型
    val coMapResultStream: DataStream[Product] = connectedStreams.map(
      waringData => (waringData._1, waringData._2, "warning"),
      lowTempData => (lowTempData.id, "healthy")
    )

    // 4.3 union合流
    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream)
    coMapResultStream.print("coMap")
    unionStream.print("unionStream")
    env.execute("TransformDemo")
  }
}




