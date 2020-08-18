package batch

import java.io.File;
import java.util;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.scala.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description:分布式缓存
 */
object BatchDisCacheDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._;
    //    1：注册缓存文件
    env.registerCachedFile("D:\\WorkSpace\\ideaProject\\flink_example\\doc\\data.txt", "b.txt");
    val data: DataSet[String] = env.fromElements("a", "b", "c", "d");
    data.map(new RichMapFunction[String, String] {
      override def open(parameters: Configuration): Unit = {
        super.open(parameters);
        //        2：获取缓存文件
        val myFile: File = getRuntimeContext.getDistributedCache.getFile("b.txt");
        val lines: util.List[String] = FileUtils.readLines(myFile);
        val it: util.Iterator[String] = lines.iterator();
        while (it.hasNext) {
          println("line: " + it.next());
        }
      }

      override def map(value: String): String = {
        return value;
      }
    }).print();
  }
}
