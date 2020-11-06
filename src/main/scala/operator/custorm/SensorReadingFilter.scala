package operator.custorm

import org.apache.flink.api.common.functions.FilterFunction;
import source.SourceDemo.SensorReading;

/**
 * Author:BYDylan
 * Date:2020/11/6
 * Description: 自定义 Filter
 */
class SensorReadingFilter extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean =
    value.id.startsWith("sensor_1");
}