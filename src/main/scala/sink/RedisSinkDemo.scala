package sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 输出到 redis
 */
object RedisSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val port = 9000
    val text = env.socketTextStream("127.0.0.1", port, '\n')
    val l_wordsData = text.map(line => ("l_words_scala", line))
    val conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build()
    val redisSink = new RedisSink[Tuple2[String, String]](conf, new MyRedisMapper)
    l_wordsData.addSink(redisSink)
    env.execute("StreamingDataToRedisScala")
  }


  class MyRedisMapper extends RedisMapper[Tuple2[String, String]] {
    override def getKeyFromData(data: (String, String)) = {
      data._1
    }

    override def getValueFromData(data: (String, String)) = {
      data._2
    }

    override def getCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }
  }
}
