package operator.custorm

import org.apache.flink.api.common.functions.Partitioner;

/**
 * Author:BYDylan
 * Date:2020/5/7
 * Description:自定义分区
 */
class ContentPartitioner extends Partitioner[Long] {
  override def partition(key: Long, numPartitions: Int) = {
    println("分区总数: " + numPartitions);
    if (key % numPartitions == 0) {
      0;
    } else {
      1;
    }
  }
}
