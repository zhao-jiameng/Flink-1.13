package sink

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._


/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: sink
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-19 23:26
 * @DESCRIPTION
 *
 */
object sinkToFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements(
      Event("Mary", "./home", 100L),
      Event("Sum", "./cart", 500L),
      Event("King", "./prod", 1000L),
      Event("King", "./root", 200L)
    )

    //TODO 直接一文本形式分布式写到文件中
    val fileSink = StreamingFileSink
      .forRowFormat(
        new Path("src/main/resources/output/fileSink"),
        new SimpleStringEncoder[String]("UTF-8")
      )
      .withRollingPolicy(  //指定滚动策略
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMinutes(15)) //十五分钟混动
          .withInactivityInterval(TimeUnit.MINUTES.toMinutes(5)) //五分钟无数据滚动
          .withMaxPartSize(1024*1024*1024)                          //最大文件大小
          .build()
      )
      .build()
    data.map(_.toString).addSink(fileSink)

    env.execute("SinkFile")
  }
}