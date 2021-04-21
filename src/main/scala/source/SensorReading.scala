package source

/**
 * Author:BYDylan
 * Date:2021/4/16
 * Description: extends java.io.Serializable
 */
case class SensorReading(var id: String, var timestamp: Long, var temperature: Double) {
//  override def toString: String = {
//    id + "," + timestamp + "," + temperature;
//  }
}
