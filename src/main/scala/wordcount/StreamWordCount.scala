package wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: wordcount
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-14 20:56
 * @DESCRIPTION
 *
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //val lineDataStream = env.socketTextStream("localhost",7777)
    //从参数提取 --host localhost --port 7777
    val parameterTool = ParameterTool.fromArgs(args)
    val host = parameterTool.get("host")
    val port = parameterTool.getInt("port")
    val lineDataStream = env.socketTextStream(host,port)
    val wordAndOne = lineDataStream.flatMap(_.split(" ")).map(word => (word, 1))
    val sum = wordAndOne.keyBy(_._1).sum(1)
    sum.print()
    env.execute("WordCount")
  }
}
