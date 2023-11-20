package source

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark

import scala.util.Random

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: source
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-19 2:20
 * @DESCRIPTION
 *
 */
//class ClickSource extends SourceFunction[Event]{  //串行执行
class ClickSource extends ParallelSourceFunction[Event]{ //并行执行
  var Running=true
  override def run(sourceContext: SourceFunction.SourceContext[Event]): Unit = {
    val random = new Random()

    val users=Array("Mary","Alice","Bob","Cary")
    val urls=Array("./home","./cart","./fav","./prod?id=1","./prod?id=2","./prod?id=3")

    while (Running){
      val event=Event(
        users(random.nextInt(users.length)),
        urls(random.nextInt(urls.length)),
        Calendar.getInstance.getTimeInMillis
      )
/*      //为要发送的数据分配时间戳
      sourceContext.collectWithTimestamp(event,event.timestamp)
      //像下游直接发送水位线
      sourceContext.emitWatermark(new Watermark(event.timestamp-1L))*/

      sourceContext.collect(event)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    Running=false
  }
}
