package window


import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import source.ClickSource

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: window
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-21 15:01
 * @DESCRIPTION
 *
 */
object window {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    val data= env.addSource(new ClickSource)
      .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
      .withTimestampAssigner(new SerializableTimestampAssigner[source.Event] {
        override def extractTimestamp(t:source.Event, l: Long): Long = t.timestamp
      }))

    data.keyBy(_.user)
//      .window(TumblingEventTimeWindows.of(Time.seconds(1)))                    //基于事件时间的滚动窗口
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))               //基于处理时间的滚动窗口
//      .window(SlidingEventTimeWindows.of(Time.minutes(1),Time.seconds(1)))     //基于事件时间的滑动窗口
//      .window(EventTimeSessionWindows.withGap(Time.seconds(1)))                //基于事件时间的会话窗口
//      .countWindow(10,2)                                                       //滑动计数窗口
    env.execute("window")
  }
}
