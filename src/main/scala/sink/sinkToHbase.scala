package sink

import java.nio.charset.StandardCharsets

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: sink
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-20 15:37
 * @DESCRIPTION
 *
 */
object sinkToHbase {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements(
      Event("Mary", "./home", 100L),
      Event("Sum", "./cart", 500L),
      Event("King", "./prod", 1000L),
      Event("King", "./root", 200L)
    )

    data.addSink(new RichSinkFunction[Event] {
      var conn:Connection=null
      override def open(parameters: Configuration): Unit = {
        val conf = HBaseConfiguration.create()
        conf.set("","")
        conn=ConnectionFactory.createConnection(conf)
      }
      override def invoke(value: Event, context: SinkFunction.Context): Unit = {
        val table=conn.getTable(TableName.valueOf("test"))
        val put = new Put("rowkey".getBytes(StandardCharsets.UTF_8))
        put.addColumn(
          "info".getBytes(StandardCharsets.UTF_8),
          "username".getBytes(StandardCharsets.UTF_8),
          value.user.getBytes(StandardCharsets.UTF_8))
        table.put(put)
        table.close()
      }
      override def close(): Unit = conn.close()
    })
    env.execute("sinkToHbase")
  }
}
