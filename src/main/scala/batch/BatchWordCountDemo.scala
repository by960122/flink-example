package batch

import org.apache.flink.api.scala._;

/**
 * Author:BYDylan
 * Date:2020/5/5
 * Description:单词计数,读取本地文件,对文件中的单词进行拆分,并且统计每个单词出现的总次数
 */
object BatchWordCountDemo {
  def main(args: Array[String]) {
    val inputPath = "D:\\WorkSpace\\ideaProject\\flink_example\\doc\\data.txt";
    val outputPath = "D:\\WorkSpace\\ideaProject\\flink_example\\doc";
    //获取执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment;
    //读取本地文件中的数据
    val text: DataSet[String] = env.readTextFile(inputPath);
    //对数据进行处理
    val wordCount: AggregateDataSet[(String, Int)] = text.flatMap(_.toLowerCase()
      .split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1);
    //    println(wordCount);
    wordCount.print();
    //把结果保存到文件中
    //    wordCount.writeAsCsv(outputPath,"\n"," ").setParallelism(1);
    //提交任务
    //    env.execute("batch word count scala");
  }
}
