package operator.custorm

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction};
import source.SourceDemo.SensorReading;

/**
 * Author:BYDylan
 * Date:2020/11/6
 * Description: 自定义 Reduce
 */

class SensorReadingReduce extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value2.timestamp, value1.temperature.min(value2.temperature));
}