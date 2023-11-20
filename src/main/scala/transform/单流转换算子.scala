package transform

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class Event(user:String,url:String,timestamp:Long)
/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: transform
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-19 2:45
 * @DESCRIPTION
 *
 */
object 单流转换算子 {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data= env.fromElements(
      Event("Mary", "./home", 100L),
      Event("Sum", "./cart", 500L),
      Event("King", "./prod", 1000L),
      Event("King", "./root", 200L)
    )
    //TODO Map
    val map=data.map(_.user).print("map")
    val mapFunction=data.map(new MapFunction[Event,String] {
      override def map(t: Event): String = t.user
    }).print("mapFunction")

    //TODO Filter
    val filter=data.filter(_.user=="Sum").print("filter")
    val filterFunction=data.filter(new FilterFunction[Event] {
      override def filter(t: Event): Boolean = t.user.contains("m") //包含
    }).print("filterFunction")

    //TODO FlatMap
    val flatMap=data.flatMap(new FlatMapFunction[Event,String] {
      override def flatMap(t: Event, collector: Collector[String]): Unit = if(t.user=="Sum") collector.collect(t.url)
    }).print("flatMapFunction")

    //TODO KeyBy
    val keyBy=data.keyBy(_.user).print("keyBy")
    val keyByFunction=data.keyBy(new KeySelector[Event,String] {
      override def getKey(in: Event): String = in.user
    })

    //TODO 简单聚合：Sun,Min,Max(抽取聚合前第一条数据),MinBy,MaxBy(抽取当前数据)
    //统计当前最大时间戳
    keyByFunction.max("timestamp").print("max")
    keyByFunction.maxBy(2).print("maxBy")
    //keyByFunction.maxBy("_2").print("maxByT")  //元组根据位置取元素

    //TODO 规约聚合 Reduce
    //提取当前最活跃用户
    data.map(data=>(data.user,1)).keyBy(_._1)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = (t._1,t._2+t1._2)
      })
      //提取所有元素
      .keyBy(data=>true)
      .reduce((state,data)=>if(state._2>=data._2) state else data)
      .print("reduceFunction")


    //TODO 富函数类测试
    data.map(new RichMapFunction[Event,Long] {
      override def open(parameters: Configuration): Unit = println("索引号为"+getRuntimeContext.getIndexOfThisSubtask+"的任务开始")
      override def close(): Unit = println("索引号为"+getRuntimeContext.getIndexOfThisSubtask+"的任务结束")
      override def map(in: Event): Long = in.timestamp
    })
   env.execute("单流转换算子")
  }
}
