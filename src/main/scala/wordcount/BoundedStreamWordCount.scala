package wordcount

import org.apache.flink.streaming.api.scala._

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: wordcount
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-14 20:47
 * @DESCRIPTION
 *
 */
object BoundedStreamWordCount {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val lineDataStream = env.readTextFile("src/main/resources/input/word.txt")
    val wordAndOne=lineDataStream.flatMap(_.split(" ")).map(word => (word,1))
    val sum=wordAndOne.keyBy(_._1).sum(1)
    sum.print()
    env.execute("WordCount")
  }
}
