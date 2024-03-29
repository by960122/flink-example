package sink

import java.io.File

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import source.SensorReading

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 输出到文件
 */

object FileSinkDemo {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val filePath = projectPath + File.separator + "sensor.txt"
    val fileStream: DataStream[String] = env.readTextFile(filePath)

    val dataStream: DataStream[SensorReading] = fileStream.map(data => {
      val arr = data.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    dataStream.print()
    //    dataStream.writeAsCsv(projectPath + File.separator + "out.txt")
    dataStream.addSink(StreamingFileSink.forRowFormat(
      new Path(projectPath + File.separator + "out1.txt"),
      new SimpleStringEncoder[SensorReading]()
    ).build())

    env.execute("FileSink")
  }
}
