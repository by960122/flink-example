package streaming.streamAPI

import java.util;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment};
import streaming.custormSource.MyNoParalleSourceScala;

/**
 * Author:BYDylan
 * Date:2020/5/6
 * Description:切分数据流
 */
object StreamingSplitDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._;
    val text: DataStream[Long] = env.addSource(new MyNoParalleSourceScala);
    val evenStream: DataStream[Long] = text.split(new OutputSelector[Long] {
      override def select(value: Long) = {
        val list = new util.ArrayList[String]();
        if (value % 2 == 0) {
          list.add("even");
        } else {
          list.add("odd");
        }
        list;
      }
    }).select("even"); //    通过select获取指定的数据流
    evenStream.print().setParallelism(1);
    env.execute("StreamingDemoSplitScala");
  }
}
