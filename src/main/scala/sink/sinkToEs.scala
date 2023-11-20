package sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import source.ClickSource

case class Event(user:String,url:String,timestamp:Long)
/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: sink
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-20 15:04
 * @DESCRIPTION
 *
 */
object sinkToEs {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements(
      Event("Mary", "./home", 100L),
      Event("Sum", "./cart", 500L),
      Event("King", "./prod", 1000L),
      Event("King", "./root", 200L)
    )

    //定义es集群主机列表
    val hosts = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("master",9200))

    //定义一个esSinkFunction
    val esFun = new ElasticsearchSinkFunction[Event] {
      override def process(t: Event, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val data = new util.HashMap[String, String]()
        data.put(t.user, t.url)
        //包装要发送的http请求
        val request = Requests.indexRequest()
          .index("clicks") //表名
          .source(data) //数据
          .`type`("event") //类型
        //发送请求
        requestIndexer.add(request)
      }
    }
    data.addSink(new ElasticsearchSink.Builder[Event](hosts,esFun).build())
    //验证命令
    //curl 'localhost:9200/_cat/indices?v'
    //curl 'localhost:9200/clicks/_search?pretty'
    env.execute("sinkRedis")
  }
}
