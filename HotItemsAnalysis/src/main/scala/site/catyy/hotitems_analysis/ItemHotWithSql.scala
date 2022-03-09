package site.catyy.hotitems_analysis

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

import java.util.Properties

/**
 *
 * @author zhangYu
 *
 */
object ItemHotWithSql {

  def main(args: Array[String]): Unit = {
    // 读取流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 临时设置并行度为1（生产环境不会这样做）
    env.setParallelism(1)
    // 设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val tableEnv = StreamTableEnvironment.create(env, settings)
    //    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // 读取数据源，从文件中读取
    //    val sourceStream = env.readTextFile("data/userBehavior.csv")

    // 自定义商品点击数据
    //    val sourceStream = env.addSource(new UserBehaviorSource())

    // 从kafka消费数据
    val props = new Properties()
    props.setProperty("bootstrap.servers", "server10:9092,server11:9092,server12:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val sourceStream = env.addSource(new FlinkKafkaConsumer010[String]("hotItems", new SimpleStringSchema(), props))

    val dataStream: DataStream[UserBehavior] = sourceStream.map(line => {
      val arr = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      // 添加水印方法一，合适数据中的事件时间戳都是顺序递增的情况
      //      .assignAscendingTimestamps(_.timestamp * 1000L)
      // 添加水印方法二
      //      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.minutes(1)) {
      //        override def extractTimestamp(t: UserBehavior): Long = t.timestamp
      //      })
      // 添加水印方法三，自定义水印规则
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[UserBehavior] {

        // 延迟时间
        val delayTime: Long = 2 * 1000L

        // 当前时间
        var currentTimestamp: Long = 0L

        // 返回当前的水印时间，水印时间需要减去延迟时间
        override def getCurrentWatermark: Watermark = {
          val watermark = new Watermark(currentTimestamp - delayTime)
          watermark
        }

        // 返回最大的时间戳
        override def extractTimestamp(t: UserBehavior, l: Long): Long = {
          currentTimestamp = Math.max(t.createTime * 1000L, currentTimestamp)
          currentTimestamp
        }
      })

    // 用sql实现top的选取
    tableEnv.createTemporaryView("aggtable", dataStream, 'itemId, 'behavior, 'createTime.rowtime)

            val resultTable: Table = tableEnv.sqlQuery(
              """
                |SELECT
                |  itemId, windowEnd, cnt
                |FROM (
                |SELECT
                | *,
                | ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY cnt DESC) AS rn
                |FROM
                |(
                |     SELECT
                |       itemId,
                |       hop_end(createTime, INTERVAL '5' minute, INTERVAL '1' hour) AS windowEnd,
                |       COUNT(itemId) AS cnt
                |     FROM
                |       aggtable
                |     WHERE behavior = 'pv'
                |     GROUP BY
                |      itemId,
                |      hop(createTime, INTERVAL '5' minute, INTERVAL '1' hour)
                |  ) AS t1
                |  ) AS t2
                |  WHERE rn <= 5
                |""".stripMargin)

    // 输出打印
    resultTable.toRetractStream[Row].print()

    env.execute("hot item sql job")
  }

}
