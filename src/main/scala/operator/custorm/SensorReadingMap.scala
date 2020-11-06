package operator.custorm

import org.apache.flink.api.common.functions.MapFunction;
import source.SourceDemo.SensorReading;

/**
 * Author:BYDylan
 * Date:2020/11/6
 * Description:
 */

class SensorReadingMap extends MapFunction[SensorReading, String] {
  override def map(value: SensorReading): String = value.id + " temperature";
}