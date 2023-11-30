package processfunction

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.{ClickSource, Event}

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: processfunction
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-23 20:46
 * @DESCRIPTION
 *
 */
object ProcessFunction {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data=env.addSource(new ClickSource).assignAscendingTimestamps(_.timestamp)

    data.process(new ProcessFunction[Event,String] {
      override def processElement(i: Event, context: ProcessFunction[Event, String]#Context, collector: Collector[String]): Unit = {
        if(i.user.equals("Mary")) collector.collect(i.user)
        else if(i.user.equals("Bob")) collector.collect(i.user+i.url)
        println(getRuntimeContext.getIndexOfThisSubtask)
        println(context.timerService().currentWatermark())
      }
    }).print()
    env.execute("processFunction")
  }
}
