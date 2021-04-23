package operator

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.scala._;

import scala.collection.mutable.ListBuffer;

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description:获取前N条元素
 */
object TopNDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment;
    val data = ListBuffer[Tuple2[Int, String]]();
    data.append((2, "zs"));
    data.append((4, "ls"));
    data.append((3, "ww"));
    data.append((1, "dz"));
    data.append((1, "jz"));
    data.append((1, "az"));
    val text: DataSet[(Int, String)] = env.fromCollection(data);
    text.first(3).print();
    println("根据数据中的第一列进行分组,获取每组的前2个元素: ");
    text.groupBy(0).first(2).print();
    println("根据数据中的第一列分组,再根据第二列进行组内排序[升序],获取每组的前2个元素: ");
    text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
    println("不分组,全局排序获取集合中的前3个元素,根据第一列正序,第二列倒序: ");
    text.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).first(3).print();
  }
}
