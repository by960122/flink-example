package operator.custorm

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import source.SensorReading

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 自定义 Reduce
 */

class SensorReadingReduce extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    new SensorReading(value1.id, value2.timestamp, value1.temperature.min(value2.temperature))
}