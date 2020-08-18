package streaming

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment};

/**
 * Author:BYDylan
 * Date:2020/5/5
 * Description:集合充当数据源
 */
object StreamingFromCollectionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    val data = List(10, 15, 20);
    import org.apache.flink.api.scala._;
    val text: DataStream[Int] = env.fromCollection(data);
    val num: DataStream[Int] = text.map(_ + 1);
    num.print().setParallelism(1);
    env.execute("StreamingFromCollectionScala");
  }
}
