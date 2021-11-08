package table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, table2RowDataSet, tableConversions}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import table.TableFromFileDemo.projectPath

import java.io.File

/**
 * Author:BYDylan
 * Date:2021/10/27
 * Description:
 */
object TableToFile {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]) {
    val filePath = projectPath + File.separator + "doc\\sensor.txt"
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n").deriveSchema())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.FLOAT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("sensor_reading_file")
    // 转换操作
    val table: Table = tableEnv.from("sensor_reading_file")
    val sensorTable: Table = table
      .select($("id"), $("timestamp"))
      .filter($("id").isEqual("sensor_1"))
      .groupBy($("id")).select($("id"), $("id").count().as("count"))

    val fileOutputPath = projectPath + File.separator + "doc\\sensor_output.txt"
    tableEnv.connect(new FileSystem().path(fileOutputPath))
      .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n").deriveSchema())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("count", DataTypes.DOUBLE()))
      .createTemporaryTable("sensor_reading_output")

    sensorTable.executeInsert("sensor_reading_output")
  }
}
