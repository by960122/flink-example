package sink

import java.io.File
import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import source.SensorReading

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 输出到 ES
 */
object EsSinkDemo {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val filePath = projectPath + File.separator + "doc\\sensor.txt"
    val fileStream: DataStream[String] = env.readTextFile(filePath)

    val dataStream: DataStream[SensorReading] = fileStream.map(data => {
      val arr = data.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 定义HttpHosts
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    // 自定义写入es的EsSinkFunction
    val elasticsearchSink: ElasticsearchSinkFunction[SensorReading] = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("id", t.id)
        dataSource.put("temperature", t.temperature.toString)
        dataSource.put("ts", t.timestamp.toString)

        // 创建index request,用于发送http请求
        val indexRequest: IndexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("readingdata")
          .source(dataSource)

        // 用indexer发送请求
        requestIndexer.add(indexRequest)
      }
    }

    dataStream.addSink(new ElasticsearchSink
    .Builder[SensorReading](httpHosts, elasticsearchSink)
      .build()
    )

    env.execute("EsSinkDemo")
  }
}
