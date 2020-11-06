package source.custorm

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

/**
 * Author:BYDylan
 * Date:2020/5/6
 * Description:创建并轻度为1的自定义source,实现从1开始产生递增数字
 */
class NoParalleSource extends SourceFunction[Long] {
  var count = 1L;
  var isRunning = true;
//  source组件执行的时候,这个方法会被调用
  override def run(ctx: SourceContext[Long]) = {
    while (isRunning) {
      ctx.collect(count);
      count += 1;
      Thread.sleep(1000);
    }
  }

  override def cancel() = {
    isRunning = false;
  }

}
