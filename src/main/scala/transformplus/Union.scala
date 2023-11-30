package transformplus

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.Event

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: splitstream
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-25 18:23
 * @DESCRIPTION
 *
 */
object Union {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1=env.socketTextStream("localhost",7777).map(data=>{
      val datas = data.split(",")
      Event(datas(0).trim,datas(1).trim,datas(2).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp)

    val stream2 = env.socketTextStream("localhost", 8888).map(data => {
      val datas = data.split(",")
      Event(datas(0).trim, datas(1).trim, datas(2).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp)

    stream1.union(stream2).process(new ProcessFunction[Event,String]{
      override def processElement(i: Event, context: ProcessFunction[Event, String]#Context, collector: Collector[String]): Unit = {
        collector.collect(s"当前水位线为：${context.timerService().currentWatermark()}")
      }
    }).print()

    env.execute("union")
  }
}
