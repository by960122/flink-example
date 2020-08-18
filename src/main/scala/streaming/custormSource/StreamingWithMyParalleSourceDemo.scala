package streaming.custormSource

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Author:BYDylan
 * Date:2020/5/6
 * Description:使用支持多并行度的source
 */
object StreamingWithMyParalleSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._;
    val text: DataStream[Long] = env.addSource(new MyParalleSourceScala).setParallelism(2);
    val mapData: DataStream[Long] = text.map(line => {
      println("接收到的数据：" + line);
      line;
    });
    val sum: DataStream[Long] = mapData.timeWindowAll(Time.seconds(2)).sum(0);
    sum.print().setParallelism(1);
    env.execute("StreamingDemoWithMyParalleSourceScala");
  }
}
