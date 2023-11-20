package wordcount

import org.apache.flink.api.scala._

/**
 *
 * @PROJECT_NAME: flink
 * @PACKAGE_NAME: wordcount
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-14 20:17
 * @DESCRIPTION
 *
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //读取文本文件内容
    val lineDataSet = env.readTextFile("src/main/resources/input/word.txt")
    //对数据集进行转换处理
    val wordAndOne = lineDataSet.flatMap(_.split(" ")).map(word => (word, 1))
    //按照单词进行分组聚合
    val value = wordAndOne.groupBy(0).sum(1)
    //对结果打印输出
    value.print()

  }

}
