package processfunction

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.{ClickSource, Event}
/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: processfunction
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-23 23:48
 * @DESCRIPTION
 *
 */
object TimeTimer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data = env.addSource(new ClickSource).assignAscendingTimestamps(_.timestamp)

    //TODO 基于处理时间的定时器
    data.keyBy(data=>"data").process(new KeyedProcessFunction[String,Event,String] {
      override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context, collector: Collector[String]): Unit = {
        val currentTime = context.timerService().currentProcessingTime()
        collector.collect("数据到达，当前时间是："+currentTime)
        context.timerService().registerProcessingTimeTimer(currentTime+5000L)
      }
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect("定时器触发，触发时间为："+timestamp)
      }
    }).print("ProcessingTimeTimer")

    //TODO 基于事件时间的定时器
    val data1=env.addSource(new EventSource).assignAscendingTimestamps(_.timestamp)
    data1.keyBy(data => "data").process(new KeyedProcessFunction[String, Event, String] {
      override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context, collector: Collector[String]): Unit = {
        val currentTime = context.timerService().currentWatermark()
        collector.collect(s"数据到达，当前时间是：$currentTime ,当前数据时间戳是 ${i.timestamp}")
        context.timerService().registerEventTimeTimer(i.timestamp + 5000L)
      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect("定时器触发，触发时间为：" + timestamp)
      }
    }).print("EventTimeTimer")

    env.execute("ProcessingTimeTimer")
  }
  class EventSource extends RichSourceFunction[Event] {
    override def run(sourceContext: SourceFunction.SourceContext[Event]): Unit = {
      sourceContext.collect(Event("Mary","./root",100L))
      Thread.sleep(5000L)
      sourceContext.collect(Event("Mary", "./root", 200L))
      Thread.sleep(5000L)
      sourceContext.collect(Event("Mary", "./root", 1000L))
      Thread.sleep(5000L)
      sourceContext.collect(Event("Mary", "./root", 6000L))
      Thread.sleep(5000L)
      sourceContext.collect(Event("Mary", "./root", 6001L))
      Thread.sleep(5000L)
    }

    override def cancel(): Unit = ???
  }
}
