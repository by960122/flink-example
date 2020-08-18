package streaming.streamAPI

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import streaming.custormSource.MyNoParalleSourceScala

/**
 * Author:BYDylan
 * Date:2020/5/6
 * Description:过滤保留满足条件的数据
 */
object StreamingFilterDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._;
    val text: DataStream[Long] = env.addSource(new MyNoParalleSourceScala);
    val filterData: DataStream[Long] = text.map(line => {
      println("原始接收到的数据：" + line);
      line;
    }).filter(_ % 2 == 0);
    val sum: DataStream[Long] = filterData.map(line => {
      println("过滤之后的数据" + line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0);
    sum.print().setParallelism(1);
    env.execute("StreamingDemoFilterScala");
  }
}
