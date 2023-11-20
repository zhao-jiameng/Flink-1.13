package source

import java.util.Properties

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.scala._

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: source
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-19 1:54
 * @DESCRIPTION
 *
 */
object SourceKafka {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaSource =
      KafkaSource.builder()
      .setBootstrapServers("")
      .setTopics("")
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
    val kafkaStream: DataStream[String] =
      env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka Source")
    kafkaStream.print("kafka")
    env.execute("SourceKafka")
  }
}
