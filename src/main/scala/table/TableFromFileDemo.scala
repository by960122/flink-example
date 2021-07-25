package table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

import java.io.File

/**
 * Author:BYDylan
 * Date:2021/7/24
 * Description: 从文件中 定义表
 */
object TableFromFileDemo {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]) {
    val filePath = projectPath + File.separator + "doc\\sensor.txt"
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //    val schema: Schema = Schema.newBuilder.column(
    //      "f0"
    //      , DataTypes.STRUCTURED(
    //        SensorReading.getClass
    //        , DataTypes.FIELD("id", DataTypes.STRING())
    //        , DataTypes.FIELD("timestamp", DataTypes.FLOAT())
    //        , DataTypes.FIELD("temperature", DataTypes.DOUBLE()))
    //    ).build
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n").deriveSchema())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.FLOAT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("sensor_reading_file")
    tableEnv.from("sensor_reading_file").execute().print()
  }
}
