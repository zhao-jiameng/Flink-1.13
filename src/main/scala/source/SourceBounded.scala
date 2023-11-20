package source

import org.apache.flink.streaming.api.scala._

case class Event(user:String,url:String,timestamp:Long)
/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: source
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-19 1:41
 * @DESCRIPTION
 *
 */
object SourceBounded {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //TODO 从元素读取数据
    env.fromElements(1, 2, 3, 4, 5).print("number")
    env.fromElements(
      Event("Mary", "./home", 100L),
      Event("Mary", "./home", 100L),
      Event("Mary", "./home", 100L)
    ).print("样例类")
    //TODO 从集合读取数据
    val list = List(
      Event("Mary", "./home", 100L),
      Event("Mary", "./home", 100L),
      Event("Mary", "./home", 100L)
    )
    env.fromCollection(list).print("list")
    //TODO 从文件读取数据
    env.readTextFile("src/main/resources/input/word.txt").print("file")

    env.execute("SourceBoundedTest")
  }
}
