package sink

import java.sql.PreparedStatement

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._

/**
 *
 * @PROJECT_NAME: flink1.13
 * @PACKAGE_NAME: sink
 * @author: 赵嘉盟-HONOR
 * @data: 2023-11-20 15:23
 * @DESCRIPTION
 *
 */
object sinkToMysql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements(
      Event("Mary", "./home", 100L),
      Event("Sum", "./cart", 500L),
      Event("King", "./prod", 1000L),
      Event("King", "./root", 200L)
    )

    data.addSink( JdbcSink.sink(
      "insert into clicks values(?,?)",
      new JdbcStatementBuilder[Event] {
        override def accept(t: PreparedStatement, u: Event): Unit = {
          t.setString(1,u.user)
          t.setString(2,u.url)
        }
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://master:3306/test")
        .withDriverName("com.jdbc.jdbc.Driver")
        .withUsername("root")
        .withPassword("root")
        .build()
    ))
    env.execute("sinkRedis")
  }
}
