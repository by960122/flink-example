package operator.custorm

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import source.SourceDemo.SensorReading;

/**
 * Author:BYDylan
 * Date:2020/11/6
 * Description: 富函数,可以获取到运行时上下文,还有一些生命周期
 */

class SensorReadingRichMap extends RichMapFunction[SensorReading, String] {
  //  做一些初始化操作,比如数据库的连接 getRuntimeContext
  override def open(parameters: Configuration): Unit = {
  }

  override def map(value: SensorReading): String = value.id + " temperature";

  //  一般做收尾工作,比如关闭连接,或者清空状态
  override def close(): Unit = {
  }
}