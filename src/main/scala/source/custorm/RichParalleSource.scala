package source.custorm

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 创建支持多并行度的自定义source
 */
class RichParalleSource extends RichParallelSourceFunction[Long] {
  var count = 1L
  var isRunning = true

  override def run(ctx: SourceContext[Long]) = {
    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel() = {
    isRunning = false
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()
}
