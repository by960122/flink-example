package operator.custorm

import org.apache.flink.api.common.functions.MapFunction
import source.SensorReading

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description:
 */

class SensorReadingMap extends MapFunction[SensorReading, String] {
  override def map(value: SensorReading): String = value.id + " temperature"
}