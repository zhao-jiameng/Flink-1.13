package window

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._


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

    env.getConfig.setAutoWatermarkInterval(500L) //设置自动生成水位线的时间间隔

    val data = env.fromElements(
      Event("Mary", "./home", 100L),
      Event("Sum", "./cart", 500L),
      Event("King", "./prod", 1000L),
      Event("King", "./root", 200L)
    )

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
  }

}
