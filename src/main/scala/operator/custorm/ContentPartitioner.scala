package operator.custorm

import org.apache.flink.api.common.functions.Partitioner

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 自定义分区
 */
class ContentPartitioner extends Partitioner[Long] {
  override def partition(key: Long, numPartitions: Int) = {
    println("分区总数: " + numPartitions)
    if (key % numPartitions == 0) {
      0
    } else {
      1
    }
  }
}
