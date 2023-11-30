package window

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class Event(user:String,url:String,timestamp:Long)
/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: window
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-20 18:54
 * @DESCRIPTION
 *
 */
object watermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(500L) //设置自动生成水位线的时间间隔

    val data = env.fromElements(
      Event("Mary", "./home", 100L),
      Event("Sum", "./cart", 500L),
      Event("King", "./prod", 1000L),
      Event("King", "./root", 200L)
    )
    //TODO 水位线生成策略
    //有序的水位线生成策略
    data.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[Event]()
      .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
      }
    ))

    //乱序的水位线生成策略
    data.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(5))
    .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
      override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
    }))

    //自定义周期水位线生成
    data.assignTimestampsAndWatermarks(new WatermarkStrategy[Event] {
      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Event] = {
        new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        }
      }
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Event] = {
        new WatermarkGenerator[Event] {
          //定义一个延迟时间
          val delay=5000L
          //定义书翔保存最大时间戳
          var maxTs=Long.MinValue + delay + 1
          override def onEvent(t: Event, l: Long, watermarkOutput: WatermarkOutput): Unit = { //传入最大时间戳
            maxTs=math.max(maxTs,t.timestamp)
            //watermarkOutput.emitWatermark(new Watermark(maxTs-delay-1))    //不使用周期定义，自己发射
          }

          override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {  //发射
            val watermark = new Watermark(maxTs-delay-1)
            watermarkOutput.emitWatermark(watermark)
          }
        }
      }
    })


    //TODO 乱序数据水位线测试

    val data1 = env.socketTextStream("localhost", 7777)
      .map(data => {
        val datas = data.split(",")
        Event(datas(0).trim, datas(1).trim, datas(2).trim.toLong)
      })

    data1.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
      })).keyBy(_.user).window(TumblingEventTimeWindows.of(Time.seconds(1))).process(new ProcessWindowFunction[Event,String,String,TimeWindow] {
      override def process(user: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
        val start=context.window.getStart
        val end=context.window.getEnd
        val count=elements.size
        val currentWatermark=context.currentWatermark
        out.collect(s"窗口 $start~$end ,用户 $user 的活跃度为：$count ,水位线现在位于：$currentWatermark")
      }
    }).print("BoundOutOfOrder")
    env.execute("watermark")
  }

}
