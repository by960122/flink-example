package streaming.custormSource

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Author:BYDylan
 * Date:2020/5/6
 * Description:使用没有并行度的source
 */
object StreamingWithMyNoParalleSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._;
    //    添加自定义的source组件
    val text: DataStream[Long] = env.addSource(new MyNoParalleSourceScala);
    val mapData: DataStream[Long] = text.map(line => {
      println("接收到的数据：" + line);
      line;
    });
//    每隔2秒进行一次求和操作
    val sum: DataStream[Long] = mapData.timeWindowAll(Time.seconds(2)).sum(0);
    sum.print().setParallelism(1);
    env.execute("StreamingDemoWithMyNoParalleSourceScala");
  }
}
