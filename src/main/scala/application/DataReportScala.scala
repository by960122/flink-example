package application

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util
import java.util.{Date, Properties}
import scala.collection.mutable.ArrayBuffer
import scala.tools.cmd.CommandLineParser.ParseException
import scala.util.Sorting

object DataReportScala {
  val logger = LoggerFactory.getLogger("DataReportScala")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    设置使用的时间类型
    env.setParallelism(5)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    开启checkpoint
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //    隐式转换代码
    import org.apache.flink.api.scala._

    //    配置source,获取kafka中的数据
    val topic = "auditLog0616"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop110:9092")
    prop.setProperty("group.id", "auditCon")
    //    kafka中的数据格式 {"dt":"2018-01-01 10:11:11","type":"shelf","username":"shenhe1","area":"AREA_US"}
    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
    val kafkaData: DataStream[String] = env.addSource(myConsumer)

    //    对数据进行清洗
    val mapData: DataStream[(Long, String, String)] = kafkaData.map(line => {
      val jsonObject = JSON.parseObject(line)
      val dt = jsonObject.getString("dt")
      var time = 0L
      try {
        val format: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        time = LocalDateTime.parse(dt, format).toInstant(ZoneOffset.of("+8")).toEpochMilli
      } catch {
        case e: ParseException => {
          logger.error("时间解析异常 dt: " + dt, e.getCause)
        }
      }

      val type1 = jsonObject.getString("type")
      val area = jsonObject.getString("area")
      (time, type1, area)
    })

    //    过滤掉异常的数据
    val filterData: DataStream[(Long, String, String)] = mapData.filter(_._1 > 0)
    //    保存迟到太久的数据
    val outputTag: OutputTag[(Long, String, String)] = new OutputTag[Tuple3[Long, String, String]]("late_data") {}

    val resultData: DataStream[(String, String, String, Long)] = filterData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, String)] {
      var currentMaxTimeStamp = 0L
      var maxOutOfOrderness = 10000L // 最大它允许的乱序时间是10s

      //      生成watermark
      override def getCurrentWatermark = {
        new Watermark(currentMaxTimeStamp - maxOutOfOrderness)
      }

      //      抽取数据的时间戳
      override def extractTimestamp(element: (Long, String, String), previousElementTimestamp: Long) = {
        val timestamp = element._1
        //        获取目前最大的时间戳
        currentMaxTimeStamp = Math.max(timestamp, currentMaxTimeStamp)
        timestamp
      }
    }).keyBy(1, 2)
      .window(TumblingEventTimeWindows.of(Time.seconds(30))) // 窗口的大小
      .allowedLateness(Time.seconds(30)) // 允许迟到30s
      .sideOutputLateData(outputTag) // 收集迟到的数据
      .apply(new WindowFunction[Tuple3[Long, String, String], Tuple4[String, String, String, Long], Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, String)], out: Collector[(String, String, String, Long)]) = {
          //          需要对窗口内的数据进行聚合计算,最终还要获取到一个time
          //          1.获取分组字段信息
          val type1 = key.getField(0).toString
          val area = key.getField(1).toString

          //          2.迭代窗口中的数据
          val it = input.iterator
          val arrBuff = ArrayBuffer[Long]()
          var count = 0
          while (it.hasNext) {
            val next = it.next()
            arrBuff.append(next._1)
            count += 1
          }

          println(Thread.currentThread().getId + ",window 触发了,数据条数：" + count)

          //          3.排序
          val arr = arrBuff.toArray
          Sorting.quickSort(arr)

          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val time = sdf.format(new Date(arr.last))

          //          4.组装结果
          val res = new Tuple4[String, String, String, Long](time, type1, area, count)
          out.collect(res)
        }
      })
    //    获取迟到太久的数据
    val sideOutput: DataStream[(Long, String, String)] = resultData.getSideOutput[Tuple3[Long, String, String]](outputTag)

    //    把迟到的数据存储到kafka中
    val outTopic = "lateLog0616"
    val outProp = new Properties()
    outProp.setProperty("bootstrap.servers", "hadoop110:9092")
    outProp.setProperty("transaction.timeout.ms", 60000 * 15 + "")

    val myProducer = new FlinkKafkaProducer[String](outTopic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), outProp, Semantic.EXACTLY_ONCE)
    sideOutput.map(tup => tup._1 + "\t" + tup._2 + "\t" + tup._3).addSink(myProducer)

    //    把计算的结果存储到es中
    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("hadoop100", 9200, "http")) //因为es在这里我们只使用了一个节点,所以写一个

    val esSinkBuilder = new ElasticsearchSink.Builder[Tuple4[String, String, String, Long]](httpHosts, new ElasticsearchSinkFunction[(String, String, String, Long)] {
      //      添加数据
      def createIndexRequest(element: Tuple4[String, String, String, Long]): IndexRequest = {
        val map = new util.HashMap[String, Any]()
        map.put("time", element._1)
        map.put("type", element._2)
        map.put("area", element._3)
        map.put("count", element._4)
        val id = element._1.replace(" ", "_") + "-" + element._2 + "-" + element._3
        return Requests.indexRequest()
          .index("auditindex")
          .`type`("audittype")
          .id(id)
          .source(map)
      }

      override def process(element: (String, String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
        requestIndexer.add(createIndexRequest(element))
      }
    })
    esSinkBuilder.setBulkFlushMaxActions(1)
    resultData.addSink(esSinkBuilder.build())
    env.execute("DataReportScala")
  }
}