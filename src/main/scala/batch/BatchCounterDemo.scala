package batch

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration;

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description:累加器
 */
object BatchCounterDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._;

    val data: DataSet[String] = env.fromElements("a", "b", "c", "d", "x", "y");
    val res: DataSet[String] = data.map(new RichMapFunction[String, String] {
      //      1：定义一个累加器
      val numLines = new IntCounter;

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //        2: 注册累加器
        getRuntimeContext.addAccumulator("num-lines", this.numLines)
      }

      override def map(value: String): String = {
        //        对计数器累加1
        this.numLines.add(1);
        return value;
      }

    }).setParallelism(4);
    res.writeAsText("D:\\WorkSpace\\ideaProject\\flink_example\\doc");

    //    res.print();

    //    3: 获取累加器的执行结果
    val num: Int = env.execute("BatchDemoCounterScala").getAccumulatorResult[Int]("num-lines");
    println(num);
  }
}
