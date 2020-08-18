package streaming

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Author:BYDylan
 * Date:2020/5/8
 * Description:Kafka作为Flink的source,消费Kafka的数据到Flink
 */
object StreamingKafkaSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._;
    val topic = "t1";
    val prop = new Properties();
    prop.setProperty("bootstrap.servers", "127.0.0.1:9092");
    prop.setProperty("group.id", "co1");
    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop);
    myConsumer.setStartFromEarliest();
    val text = env.addSource(myConsumer);
    env.execute("StreamingKafkaSourceScala");
  }
}
