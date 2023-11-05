package operator.custorm

import org.apache.flink.api.common.functions.FilterFunction
import source.SensorReading

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 自定义 Filter
 */
class SensorReadingFilter extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean =
    value.id.startsWith("sensor_1")
}