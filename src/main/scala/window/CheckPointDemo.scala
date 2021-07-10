package window

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows

/**
 * Author:BYDylan
 * Date:2020/5/8
 * Description:设置缓存点
 */
object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val port = try
      ParameterTool.fromArgs(args).getInt("port")
    catch {
      case e: Exception => {
        System.err.println("No Port set,use default 8888")
      }
        8888
    }
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    设置checkpoint相关配置
    //    每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(1000)
    //    高级选项:
    //    设置模式为exactly-once: 这是默认值
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //    确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    //    检查点必须在一分钟内完成,或者被丢弃【checkpoint的超时时间】
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //    同一时间只允许进行一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //    表示一旦Flink处理程序被cancel后,会保留Checkpoint数据,以便根据实际需要恢复到指定的Checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //    设置statebackend
    //    env.setStateBackend(new MemoryStateBackend())
    //    env.setStateBackend(new FsStateBackend("hdfs://127.0.0.1:9000/flink/checkpoint"))
    env.setStateBackend(new RocksDBStateBackend("hdfs://127.0.0.1:8888/flink/checkpoint", true))

    //    指定数据源(socket)
    val text: DataStream[String] = env.socketTextStream("127.0.0.1", port, '\n')

    val windowCount: DataStream[WordWithCount] = text.flatMap(l => l.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2), Time.seconds(1)))
      .sum("count")
    //.reduce((a,b)=>WordWithCount(a.word,a.count+b.count))

    windowCount.print().setParallelism(1)
    env.execute("SocketWindowCount")
  }

  case class WordWithCount(word: String, count: Long)
}
