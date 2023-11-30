package window

import java.text.SimpleDateFormat

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import source.ClickSource

case class UrlViewCount(url:String,count:Long,widowStart:String,windowEnd:String)
/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: window
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-23 0:21
 * @DESCRIPTION
 *
 */
object windowFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data = env.addSource(new ClickSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner[source.Event] {
          override def extractTimestamp(t: source.Event, l: Long): Long = t.timestamp
        }))
    //TODO 增量聚合函数 Reduce(流处理)
    data.map(data => (data.user,1)).keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce((state,data)=>(data._1,data._2+state._2)).print("reduceLmd")

    data.map(data => (data.user, 1)).keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = (t._1,t._2+t1._2)
      }).print("reduceFunction")

    val data2 = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp) //升序数据时间戳提取
      .keyBy(data => "kry").window(TumblingEventTimeWindows.of(Time.seconds(10)))
    //TODO 增量聚合函数 Aggregate（统计PV，UV，输出PV/UV）
      data2
      .aggregate(new AggregateFunction[source.Event,(Long,Set[String]),Double] {
        override def createAccumulator(): (Long, Set[String]) = (0L,Set[String]())
        override def add(in: source.Event, acc: (Long, Set[String])): (Long, Set[String]) = (acc._1+1,acc._2+in.user)
        override def getResult(acc: (Long, Set[String])): Double = acc._1.toDouble/acc._2.size
        override def merge(acc: (Long, Set[String]), acc1: (Long, Set[String])): (Long, Set[String]) = ???
      }).print("aggregate")

    //TODO 全窗口函数 windowFunction（apply）（批处理）
    data2.apply(new WindowFunction[source.Event,String,String,TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[source.Event], out: Collector[String]): Unit = {
        var userSet=Set[String]()
        input.foreach(userSet+=_.user)
        val uv=userSet.size
        val windowStart=window.getStart
        val windowEnd=window.getEnd
        out.collect(s"窗口 $windowStart ~ $windowEnd 的UV值为 $uv")
      }
    }).print("windowFunction")

    data2.apply((key,window,input,out:Collector[String])=>{
      var userSet = Set[String]()
      input.foreach(userSet += _.user)
      val uv = userSet.size
      val windowStart = window.getStart
      val windowEnd = window.getEnd
      out.collect(s"窗口 $windowStart ~ $windowEnd 的UV值为 $uv")
    })

    //TODO 全窗口函数 processWindowFunction（process）
    data2.process(new ProcessWindowFunction[source.Event,String,String,TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[source.Event], out: Collector[String]): Unit = {
        var userSet = Set[String]()
        elements.foreach(userSet += _.user)
        val uv = userSet.size
        val windowStart = context.window.getStart
        val windowEnd = context.window.getEnd
        out.collect(s"窗口 $windowStart ~ $windowEnd 的UV值为 $uv")
      }
    }).print("processWindowFunction")

    //TODO 增量聚合函数与券窗口函数的结合使用：分组计算PV
    data2.aggregate(new AggregateFunction[source.Event,Long,Long] {
      override def createAccumulator(): Long = 0L
      override def add(in: source.Event, acc: Long): Long = acc+1
      override def getResult(acc: Long): Long = acc
      override def merge(acc: Long, acc1: Long): Long = ???
    },new ProcessWindowFunction[Long,UrlViewCount,String,TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
        out.collect(UrlViewCount(key,
          elements.iterator.next(),
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window.getStart),
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window.getEnd)
        ))
      }
    }).print("windowFunctionPlusMax")
    env.execute("windowFunction")
  }

}
