package sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: sink
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-19 23:46
 * @DESCRIPTION
 *
 */
object sinkToKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val data = env.fromElements(
      Event("Mary", "./home", 100L),
      Event("Sum", "./cart", 500L),
      Event("King", "./prod", 1000L),
      Event("King", "./root", 200L)
    )

    data.map(_.toString).addSink(new FlinkKafkaProducer[String]("","",new SimpleStringSchema()))

    val kafkaSink=KafkaSink.builder()
      .setBootstrapServers("")
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
      .setTopic("")
      .setValueSerializationSchema(new SimpleStringSchema())
      .build()
      )
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()

    data.map(_.toString).sinkTo(kafkaSink)

    env.execute("sinkKafka")

  }
}
