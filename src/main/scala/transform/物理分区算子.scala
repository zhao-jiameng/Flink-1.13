package transform

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import source.ClickSource

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: transform
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-19 3:56
 * @DESCRIPTION
 *
 */
object 物理分区算子 {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    val data=env.addSource(new ClickSource)

    //洗牌（随机）
    data.shuffle.print("shuffle").setParallelism(4)
    //发牌（轮询）
    data.rebalance.print("rebalance").setParallelism(4)
    //发牌（分组轮询）
    val dataRescale=env.addSource(new RichParallelSourceFunction[Int] {
      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        for(i <- 0 to 7){
          if(getRuntimeContext.getIndexOfThisSubtask==(i+1)%2) sourceContext.collect(i+1)
        }
      }
      override def cancel(): Unit = ???
    })
    dataRescale.rescale.print("rescale").setParallelism(4)
    //广播(复制多份)
    data.broadcast.print("broadcast").setParallelism(4)
    //全局分区（所有数据分发到第一个并行子程序）
    data.global.print("global").setParallelism(4)
    //自定义分区(分区器：指定分区，选择器：提取当前分区字段)
    val dataPartition=env.fromElements(1,2,3,4,5,6,7,8)
    dataPartition.partitionCustom(new Partitioner[Int] {
      override def partition(k: Int, i: Int): Int = k%2
    },data=>data).print("partitionCustom").setParallelism(4)

    env.execute("物理分区算子")
  }
}
