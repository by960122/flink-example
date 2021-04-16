package sink

import java.io.File;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.scala._;
import source.SensorReading;

/**
 * Author:BYDylan
 * Date:2020/11/6
 * Description: 输出到文件
 */

object FileSinkDemo {
  private val project_path: String = System.getProperty("user.dir");

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(1);
    val filePath = project_path + File.separator + "sensor.txt";
    val fileStream: DataStream[String] = env.readTextFile(filePath);

    val dataStream: DataStream[SensorReading] = fileStream.map(data => {
      val arr = data.split(",");
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble);
    });

    dataStream.print();
    //    dataStream.writeAsCsv(project_path + File.separator + "out.txt");
    dataStream.addSink(StreamingFileSink.forRowFormat(
      new Path(project_path + File.separator + "\\out1.txt"),
      new SimpleStringEncoder[SensorReading]()
    ).build());

    env.execute("FileSink");
  }
}
