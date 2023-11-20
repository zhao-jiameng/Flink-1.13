package source

import org.apache.flink.streaming.api.scala._

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: source
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-19 2:31
 * @DESCRIPTION
 *
 */
object SourceCustom {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    val Stream=env.addSource(new ClickSource).print()

    env.execute("SourceCustom")
  }

}
