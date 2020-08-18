package streaming.streamAPI

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import streaming.custormSource.MyNoParalleSourceScala

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description:使用自定义分区
 */
object StreamingMyPartitionerDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(2);
    import org.apache.flink.api.scala._;
    val text = env.addSource(new MyNoParalleSourceScala);

    //    把long类型的数据转成tuple类型,注意tuple1这种写法
    val tupleData = text.map(line => {
      Tuple1(line);
    })
    //    对数据进行自定义分区
    val partitionData: DataStream[Tuple1[Long]] = tupleData.partitionCustom(new MyPartitionerScala, 0);
    val result: DataStream[Long] = partitionData.map(line => {
      println("当前线程id: " + Thread.currentThread().getId + ",value:" + line._1);
      line._1;
    });
    result.print().setParallelism(1);
    env.execute("StreamingDemoMyPartitionerScala");
  }
}
