package table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * Author:BYDylan
 * Date:2021/7/24
 * Description: 新旧版本流批处理差异
 */
object TableEnvironmentDemo {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    1.1 基于老版本 planner 的流处理
    //    val oldStreamingSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    //    val environment: StreamTableEnvironment = StreamTableEnvironment.create(env, oldStreamingSettings)
    //    1.2 基于老版本 planner 的批处理
    //    val oldBatchSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inBatchMode().build()
    //    val oldBatchSettings: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    BatchTableEnvironment.create(oldBatchSettings)
    //    1.3 基于 blink planner 的流处理
    //    val blinkStreamingSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    //    val environment: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamingSettings)
    //    1.4 基于 blink planner 的批处理
    val blinkBatchgSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val environment: TableEnvironment = TableEnvironment.create(blinkBatchgSettings)
  }
}
