package table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

import java.io.File

/**
 * Author:BYDylan
 * Date:2021/7/24
 * Description: 从 kafka 定义表
 */
object TableFromKafkaDemo {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.connect(new Kafka()
      .version("2.8.1")
      .topic("sensor")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.services", "localhost:9092"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.FLOAT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("sensor_reading_kafka")
    tableEnv.from("sensor_reading_kafka").execute().print()
  }
}
