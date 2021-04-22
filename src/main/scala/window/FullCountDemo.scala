package window

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment};
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.scala._;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

/**
 * Author:BYDylan
 * Date:2020/5/8
 * Description:全量聚合
 */
object FullCountDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    val text: DataStream[String] = env.socketTextStream("127.0.0.1", 8888, '\n');

    val windowCount: DataStream[String] = text.flatMap(l => l.split("\\s"))
      .map((_, 1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
      .process(new ProcessWindowFunction[Tuple2[String, Int], String, Tuple, TimeWindow] {
        @scala.throws[Exception]
        override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[String]) = {
          println("执行process");
          var count = 0;
          for (item <- elements) {
            count += 1;
          }
          out.collect("window: " + context.window + ",count: " + count);
        }
      });
    windowCount.print().setParallelism(1);
    env.execute("SocketWindowCount");
  }
}
