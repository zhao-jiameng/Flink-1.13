package processfunction

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.hadoop.metrics2.util.Metrics2Util.TopN
import source.{ClickSource, Event}

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

case class UrlViewCount(url:String,count:Long,widowStart:Long,windowEnd:Long)
/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: processfunction
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-24 21:55
 * @DESCRIPTION
 *
 */
object TopNKeyedProcessFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data = env.addSource(new ClickSource).assignAscendingTimestamps(_.timestamp)

    val urlCountStream = data.keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .aggregate(new UrlViewCountAgg, new UrlViewCountResult)

    urlCountStream.keyBy(_.windowEnd).process(new TopN(5)).print()

    env.execute("TopNDemo2")
  }
  class UrlViewCountAgg extends AggregateFunction[source.Event,Long,Long] {
    override def createAccumulator(): Long = 0L
    override def add(in: Event, acc: Long): Long = acc+1
    override def getResult(acc: Long): Long = acc
    override def merge(acc: Long, acc1: Long): Long = ???
  }
  class UrlViewCountResult extends ProcessWindowFunction[Long,UrlViewCount,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(
        key,elements.iterator.next(),context.window.getStart,context.window.getEnd
      ))
    }
  }
  class TopN(topN:Int) extends KeyedProcessFunction[Long,UrlViewCount,String] {
    var urlViewCountListState:ListState[UrlViewCount]=_
    override def open(parameters: Configuration): Unit = {
      urlViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("list-state", classOf[UrlViewCount]))
    }
    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      urlViewCountListState.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd+1)
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val topNList = urlViewCountListState.get().toList.sortBy(-_.count).take(topN)
      val builder = new StringBuilder()
      builder.append(s"========窗口：${timestamp-1-10000} ~ ${timestamp-1} ======= \n")
      for (i <- topNList.indices){
        val urlViewCount = topNList(i)
        builder.append(
          s"浏览量Top ${i+1} " +
          s"url: ${urlViewCount.url} " +
          s"浏览量是： ${urlViewCount.count} \n")
      }
      out.collect(builder.toString())
    }
  }
}
