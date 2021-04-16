package operator

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment};
import org.apache.flink.util.Collector;
import source.custorm.NoParalleSource;

/**
 * Author:BYDylan
 * Date:2020/5/6
 * Description:切分数据流
 */
object SplitDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._;
    val text: DataStream[Long] = env.addSource(new NoParalleSource)

    val evenTag: OutputTag[Any] = new OutputTag[Any]("even");
    val oddTag: OutputTag[Any] = new OutputTag[Any]("odd");
    val splitStream: DataStream[Long] = text.process(new ProcessFunction[Long, Long] {
      override def processElement(value: Long, context: ProcessFunction[Long, Long]#Context, collector: Collector[Long]) = {
        if (value % 2 == 0) context.output(evenTag, "");
        else context.output(oddTag, "");
      }
    });

    splitStream.getSideOutput(evenTag).print().setParallelism(1);
    env.execute("StreamingDemoSplitScala");
  }
}