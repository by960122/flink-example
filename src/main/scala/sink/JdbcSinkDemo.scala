package sink

import org.apache.flink.streaming.api.scala._
import sink.custorm.JdbcSink
import source.SensorReading
import source.custorm.SensorSource

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 输出到 JDBC
 */
object JdbcSinkDemo {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val custormStream: DataStream[SensorReading] = env.addSource(new SensorSource)

    custormStream.addSink(new JdbcSink)

    env.execute("JdbcSinkDemo")
  }
}

