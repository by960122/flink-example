package operator

import operator.custorm.ContentPartitioner;
import org.apache.flink.streaming.api.scala._;
import source.custorm.NoParalleSource;

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description: 使用自定义分区
 */
object PartitionerDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(2);
    val text = env.addSource(new NoParalleSource);

    //    把long类型的数据转成tuple类型,注意tuple1这种写法
    val tupleData = text.map(line => {
      Tuple1(line);
    })
    //    对数据进行自定义分区
    val partitionData: DataStream[Tuple1[Long]] = tupleData.partitionCustom(new ContentPartitioner, 0);
    val result: DataStream[Long] = partitionData.map(line => {
      println("当前线程id: " + Thread.currentThread().getId + ",value:" + line._1);
      line._1;
    });
    result.print().setParallelism(1);
    env.execute("StreamingDemoMyPartitionerScala");
  }
}
