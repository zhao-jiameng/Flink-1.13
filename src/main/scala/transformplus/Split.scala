package transformplus

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.{ClickSource, Event}

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: splitstream
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-25 17:51
 * @DESCRIPTION
 *
 */
object Split {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data=env.addSource(new ClickSource).assignAscendingTimestamps(_.timestamp)

    //TODO 使用filter进行分流
    data.filter(_.user == "Mary").print("mary_stream")
    data.filter(_.user == "Bob").print("bob_stream")
    data.filter(data=>data.user != "Mary" && data.user != "Bob").print("else_stream")

    //TODO 使用测输出流进行分流
    val mary_tag=OutputTag[(String,String,Long)]("mary_tag")
    val bob_tag=OutputTag[(String,String,Long)]("bob_tag")

    val outputStream = data.process(new ProcessFunction[Event, Event] {
      override def processElement(i: Event, context: ProcessFunction[Event, Event]#Context, collector: Collector[Event]): Unit = {
        i.user match {
          case "Mary" => context.output(mary_tag,(i.user,i.url,i.timestamp))
          case "Bob" => context.output(bob_tag,(i.user,i.url,i.timestamp))
          case _ => collector.collect(i)
        }
      }
    })

    outputStream.print("else_tag")
    outputStream.getSideOutput(mary_tag).print("mary_tag")
    outputStream.getSideOutput(bob_tag).print("bob_tag")

    env.execute("splitStream")
  }

}
