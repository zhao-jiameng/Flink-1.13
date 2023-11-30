package processfunction

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import source.ClickSource

import scala.collection.mutable


/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: processfunction
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-24 21:32
 * @DESCRIPTION
 *
 */
object TopNProcessAllWindowExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data = env.addSource(new ClickSource).assignAscendingTimestamps(_.timestamp)

    data.map(_.url).windowAll(TumblingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .process(new ProcessAllWindowFunction[String,String,TimeWindow] {
        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          val urlCountMap=mutable.Map[String,Int]()
          elements.foreach(data=>{
            urlCountMap.get(data) match {
              case Some(value) => urlCountMap.put(data,value+1)
              case None => urlCountMap.put(data,1)
            }
          })
          val tuples = urlCountMap.toList.sortBy(-_._2).take(10)
          val builder = new StringBuilder()
          builder.append(s"=========窗口：${context.window.getStart} ~ ${context.window.getEnd}========\n")
          for (i <- tuples.indices){
            val tuple = tuples(i)
            builder.append(s"浏览量Top ${i+1} url:${tuple._1} 浏览量是： ${tuple._2} \n")
          }
          out.collect(builder.toString())
        }
      }).print()

    env.execute("TopNDemo1")
  }
}
