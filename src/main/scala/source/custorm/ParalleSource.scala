package source.custorm

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
 * Author:BYDylan
 * Date:2020/5/6
 * Description:创建支持多并行度的自定义source
 */
class ParalleSource extends ParallelSourceFunction[Long] {
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

}
