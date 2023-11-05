package source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable;

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 自定义Redis Source
 */
class MyRedisSourceScala extends SourceFunction[mutable.Map[String, String]] {
  val logger = LoggerFactory.getLogger("source.MyRedisSourceScala");
  val SLEEP_MILLION = 60000;
  val hostName = "127.0.0.1";
  val port = 6379;
  //  隐式转换,把java中的map转为scala中的map

  import scala.collection.JavaConverters._;
  var isRunning = true;
  var jedis: Jedis = _;
  //  定义一个map,存储所有国家和大区的对应关系
  var keyValueMap = mutable.Map[String, String]();

  override def run(ctx: SourceContext[mutable.Map[String, String]]) = {
    //    需要从redis中获取数据,把数据组装到map里面
    jedis = new Jedis(hostName, port);
    while (isRunning) {
      try {
        keyValueMap.clear();
        val map: mutable.Map[String, String] = jedis.hgetAll("areas").asScala;
        for (key <- map.keys.toList) {
          val value = map.get(key).get;
          val splits = value.split(",");
          for (split <- splits) {
            keyValueMap += (split -> key);
          }
        }
        if (keyValueMap.nonEmpty) {
          ctx.collect(keyValueMap);
        } else {
          logger.warn("从redis中获取的数据为空！！！");
        }
        Thread.sleep(SLEEP_MILLION);
      } catch {
        case e: JedisConnectionException => {
          logger.error("redis链接获取异常,重新获取链接: ", e.getCause);
          jedis = new Jedis(hostName, port);
        }
        case e: Exception => {
          logger.error("source数据源异常", e);
        }
      }
    }
  }

  //  当停止flink任务的时候会被调用
  override def cancel() = {
    isRunning = false;
    if (jedis != null) {
      jedis.close();
    }
  }
}
