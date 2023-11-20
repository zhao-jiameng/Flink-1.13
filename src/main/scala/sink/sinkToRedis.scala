package sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import source.ClickSource

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: sink
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-20 14:53
 * @DESCRIPTION
 *
 */
object sinkToRedis {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.addSource(new ClickSource)

    val conf=new FlinkJedisPoolConfig.Builder().setHost("").build()
    data.addSink(new RedisSink[source.Event](conf,new RedisMapper[source.Event] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"click")
      override def getKeyFromData(t: source.Event): String = t.user
      override def getValueFromData(t: source.Event): String = t.url
    }))
    env.execute("sinkRedis")
  }
}
