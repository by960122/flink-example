package source

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: extends java.io.Serializable
 */
case class SensorReading(var id: String, var timestamp: Long, var temperature: Double) {
//  override def toString: String = {
//    id + "," + timestamp + "," + temperature
//  }
}
