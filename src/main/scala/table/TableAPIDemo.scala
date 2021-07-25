package table

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import source.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.slf4j.{Logger, LoggerFactory}

import java.io.File

/**
 * Author:BYDylan
 * Date:2021/7/24
 * Description: Table api 基本使用
 */
object TableAPIDemo {
  private val projectPath: String = System.getProperty("user.dir")
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val filePath = projectPath + File.separator + "doc\\sensor.txt"
    val fileStream: DataStream[String] = env.readTextFile(filePath)
    val dataStream: DataStream[SensorReading] = fileStream.flatMap(l => l.split("\\s"))
      .map(data => {
        val arr = data.split(",")
        new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val dataTable: Table = tableEnv.fromDataStream(dataStream)
    //    logger.info("使用算子")
    //    dataTable.select("id,temperature").filter("id == 'sensor_1'").execute().print()
    logger.info("使用sql")
    tableEnv.createTemporaryView("dataTable", dataTable)
    tableEnv.sqlQuery("select id,temperature from dataTable where id = 'sensor_1'").execute().print()
  }
}
