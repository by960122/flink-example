package sink

import java.lang;
import java.util.Properties;

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment};
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaContextAware, KafkaSerializationSchema};
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Author:BYDylan
 * Date:2020/5/8
 * Description: 输出到 kafka
 */
object KafkaSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    env.enableCheckpointing(5000);
    val text: DataStream[String] = env.socketTextStream("127.0.0.1", 9001, '\n');
    val topic = "t1";
    val prop = new Properties();
    prop.setProperty("bootstrap.servers", "127.0.0.1:9092");
    //    开启了checkpoint,并且设置为仅一次语义之后,会报下面的错
    //    The transaction timeout is larger than the maximum value allowed by the broker (as configured by max.transaction.timeout.ms).
    //    第一种解决方案,FlinkKafkaProducer011里面的事务超时时间
    prop.setProperty("transaction.timeout.ms", 60000 * 15 + "");

    //    第二种解决方案,设置kafka的最大事务超时时间
    //    修改server.properties 增加一行配置transaction.max.timeout.ms=3600000

    val producer = new FlinkKafkaProducer[String](topic, new MyKafkaSerializationSchema2(topic + "_out"), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    //    过期了
    //    val myProducer = new FlinkKafkaProducer[String](topic, new SimpleStringSchema, prop)
    text.addSink(producer);
    env.execute("KafkaSinkDemo");
  }

  class MyKafkaSerializationSchema2(val topic: String) extends KafkaSerializationSchema[String] with KafkaContextAware[String] {
    override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord[Array[Byte], Array[Byte]](topic, element.toString.getBytes);

    override def getTargetTopic(t: String): Predef.String = null;
  }

}
