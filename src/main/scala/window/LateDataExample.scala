package window

import java.text.SimpleDateFormat
import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: window
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-23 19:51
 * @DESCRIPTION
 *
 */
object LateDataExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //TODO 处理迟到数据的三种方式
    //定义一个测输出流标签
    val outputTag=OutputTag[Event]("late-data")

    val data1 = env.socketTextStream("localhost", 7777)
      .map(data => {
        val datas = data.split(",")
        Event(datas(0).trim, datas(1).trim, datas(2).trim.toLong)
      })
    //TODO 一重保证：Watermark
    val result = data1.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
      })).keyBy(_.user).window(TumblingEventTimeWindows.of(Time.seconds(10)))
      //TODO 二重保证：指定窗口允许等待时间
      .allowedLateness(Time.minutes(1))
      //TODO 三重保证：将迟到数据输出到侧输出流
      .sideOutputLateData(outputTag)
      .aggregate(new AggregateFunction[Event, Long, Long] {
        override def createAccumulator(): Long = 0L
        override def add(in: Event, acc: Long): Long = acc + 1
        override def getResult(acc: Long): Long = acc
        override def merge(acc: Long, acc1: Long): Long = ???
      }, new ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
          out.collect(UrlViewCount(key,
            elements.iterator.next(),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window.getStart),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window.getEnd)
          ))
        }
      })

    result.print("result")
    data1.print("data ")
    result.getSideOutput(outputTag).print("late-data")
    env.execute("watermark")
  }

}
