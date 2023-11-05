package application

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector

import java.util.Properties
import scala.collection.mutable

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description:
 */
object DataCleanScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.setParallelism(5)
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //    设置statebackend
    env.setStateBackend(new RocksDBStateBackend("hdfs://127.0.0.1:9000/flink/checkpoint", true))

    //    定义kafkaSource
    val topic = "allData0615"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "127.0.0.1:9092")
    prop.setProperty("group.id", "con0615")
    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
    val kafkaData = env.addSource(myConsumer)
    //    redis中的数据,可以把数据发送到后面算子的所有并行实例中
    val redisData = env.addSource(new source.MyRedisSourceScala).broadcast

    val resData = kafkaData.connect(redisData).flatMap(new CoFlatMapFunction[String, mutable.Map[String, String], String] {
      var allMap = mutable.Map[String, String]()

      //      处理的kafka中的数据
      override def flatMap1(value: String, out: Collector[String]) = {
        val jsonObject = JSON.parseObject(value)
        val dt = jsonObject.getString("dt")
        val countryCode = jsonObject.getString("countryCode")
        //        获取大区,使用getOrElse 避免空指针异常
        val area = allMap.get(countryCode).getOrElse("other")
        val jsonArray = jsonObject.getJSONArray("data")
        for (i <- 0 to jsonArray.size() - 1) {
          val jsonObject1 = jsonArray.getJSONObject(i)
          jsonObject1.put("area", area)
          jsonObject1.put("dt", dt)
          out.collect(jsonObject1.toString)
        }
      }

      //      处理的是redis中的数据
      override def flatMap2(value: mutable.Map[String, String], out: Collector[String]) = {
        this.allMap = value
        println(allMap)
      }
    })
    val outTopic = "allDataClean0615"
    val outProp = new Properties()
    outProp.setProperty("bootstrap.servers", "127.0.0.1:9092")
    //    设置事务超时时间
    outProp.setProperty("transaction.timeout.ms", 60000 * 15 + "")
    val myProducer = new FlinkKafkaProducer[String](outTopic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), outProp, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
    resData.addSink(myProducer)
    env.execute("DataCleanScala")
  }

}