package operator

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment};
import source.custorm.NoParalleSource;

/**
 * Author:BYDylan
 * Date:2020/5/6
 * Description:合并,限制2个,不限制类型
 */
object ConnectDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._;
    val text1: DataStream[Long] = env.addSource(new NoParalleSource);
    val text2: DataStream[Long] = env.addSource(new NoParalleSource);
    val text2_str: DataStream[String] = text2.map("str" + _);
    val connectStream: ConnectedStreams[Long, String] = text1.connect(text2_str);
    val result = connectStream.map(line1 => {
      println("line1: " + line1);
      line1;
    }, line2 => {
      println("line2: " + line2);
      line2;
    })
    result.print().setParallelism(1);
    env.execute("StreamingDemoConnectScala");
  }
}
