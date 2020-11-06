package source

import java.io.File;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.scala._;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Author:BYDylan
 * Date:2020/11/6
 * Description: 各种 Source
 */


object SourceDemo {

  case class SensorReading(id: String, timestamp: Long, temperature: Double)

  private val project_path: String = System.getProperty("user.dir");

  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(1);
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    );
    //    从集合中读取数据
    val collectStream: DataStream[SensorReading] = env.fromCollection(dataList);

    //    从文件中读取数据
    val filePath: String = project_path + File.separator + "sensor.txt";
    val fileStream: DataStream[String] = env.readTextFile(filePath);

    //    从kafka中读取数据
    val topic = "t1";
    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "consumer-group");
    val comsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties);
    comsumer.setStartFromEarliest();
    val kafkaStream: DataStream[String] = env.addSource(comsumer);

    env.execute("SourceDemo");
  }
}
