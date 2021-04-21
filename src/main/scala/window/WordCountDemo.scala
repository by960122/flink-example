package window

import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingProcessingTimeWindows};

/**
 * Author:BYDylan
 * Date:2020/5/4
 * Description:手工通过socket实时产生一些单词,使用flink实时接收数据,对指定时间窗口内(例如：2秒)的数据进行聚合统计,并且把时间窗口内计算的结果打印出来
 * 流处理:统计单位时间内,注意 timeWindow的第二个参数,必须在这个时间内的数据才会进行计算
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    val text: DataStream[String] = env.socketTextStream("127.0.0.1", 8888, '\n');
    val windowCount: DataStream[WordWithCount] = text.flatMap(l => l.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(3))) // 滚动窗口
      .window(TumblingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1))) // 滑动事件窗口
//      .window(EventTimeSessionWindows.withGap(Time.seconds(3))) // 会话窗口
      //            .sum("count");
      .reduce((a, b) => WordWithCount(a.word, a.count + b.count));

    //  把数据打印到控制台
    val value: DataStreamSink[WordWithCount] = windowCount.print().setParallelism(1);
    //  执行任务
    env.execute("SocketWindowCount");
  }

  case class WordWithCount(word: String, count: Long);

}
