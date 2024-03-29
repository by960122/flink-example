package source

import java.io.File
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 各种 Source
 */


object SourceDemo {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dataList = List(
      new SensorReading("sensor_1", 1547718199, 35.8),
      new SensorReading("sensor_6", 1547718201, 15.4),
      new SensorReading("sensor_7", 1547718202, 6.7),
      new SensorReading("sensor_10", 1547718205, 38.1)
    )
    //    从集合中读取数据
    val collectStream: DataStream[SensorReading] = env.fromCollection(dataList)

    //    从文件中读取数据
    val filePath: String = projectPath + File.separator + "\\doc\\sensor.txt"
    val fileStream: DataStream[String] = env.readTextFile(filePath)

    //    从kafka中读取数据
    val topic: String = "t1"
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    val comsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    comsumer.setStartFromEarliest()
    val kafkaStream: DataStream[String] = env.addSource(comsumer)
    env.execute("SourceDemo")
  }
}
