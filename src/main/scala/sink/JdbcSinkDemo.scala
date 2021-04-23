package sink

import org.apache.flink.streaming.api.scala._;
import sink.custorm.JdbcSink;
import source.SensorReading;
import source.custorm.SensorSource;

/**
 * Author:BYDylan
 * Date:2020/11/6
 * Description: 输出到 JDBC
 */
object JdbcSinkDemo {
  private val projectPath: String = System.getProperty("user.dir");

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(1);

    val custormStream: DataStream[SensorReading] = env.addSource(new SensorSource);

    custormStream.addSink(new JdbcSink);

    env.execute("JdbcSinkDemo");
  }
}

