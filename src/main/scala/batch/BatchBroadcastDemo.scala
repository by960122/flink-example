package batch

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.scala.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import scala.collection.mutable.ListBuffer;

/**
 * Author:BYDylan
 * Date:2020/5/5
 * Description:广播变量
 */
object BatchBroadcastDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._;
    //    1:初始化需要广播的数据
    val broadData: ListBuffer[(String, Int)] = ListBuffer[Tuple2[String, Int]]();
    broadData.append(("zs", 18));
    broadData.append(("ls", 20));
    broadData.append(("ww", 17));
    val tupleData: DataSet[(String, Int)] = env.fromCollection(broadData);
    //    把数据转成map类型的
    val toBroadcastData: DataSet[Map[String, Int]] = tupleData.map(tup => {
      Map(tup._1 -> tup._2);
    });

    val text: DataSet[String] = env.fromElements("zs", "ls", "ww");
    val result: DataSet[String] = text.map(new RichMapFunction[String, String] {
      var allMap = Map[String, Int]();
      override def open(parameters: Configuration): Unit = {
        super.open(parameters);
        //        3：获取广播变量的数据
        val listData = getRuntimeContext.getBroadcastVariable[Map[String, Int]]("broadcastMapName");
        val it = listData.iterator();
        while (it.hasNext) {
          val next = it.next();
          allMap = allMap.++(next);
        }
      }

      override def map(value: String) = {
        val age = allMap.get(value).get;
        value + "," + age;
      }
    }).withBroadcastSet(toBroadcastData, "broadcastMapName"); //2：广播数据,给广播变量定义名称
    result.print();
    //    env.execute("broadcastMapName");
  }
}
