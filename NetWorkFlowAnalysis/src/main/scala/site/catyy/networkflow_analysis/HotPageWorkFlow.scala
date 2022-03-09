package site.catyy.networkflow_analysis


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

/**
 * 基于服务器log的热门页面浏览量统计
 *
 * @author zhangYu
 *
 */
object HotPageWorkFlow {

  def main(args: Array[String]): Unit = {

    // 构建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间语义为事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "server10:9092,server11:9092,server12:9092")
    properties.setProperty("group.id", "page-consumer")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    // 读取数据源，从kafka中读取
    val kafkaDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("page_visit", new SimpleStringSchema(), properties))

    // 数据转换
    val dataStream = kafkaDataStream
      .map(line => {
        val arr = line.split(" ")
        val format = new SimpleDateFormat("dd/MM/yyy:HH:mm:ss")
        val visitTime = format.parse(arr(3)).getTime

        PageVisitEvent(arr(0), visitTime, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PageVisitEvent](Time.seconds(1)) {
        override def extractTimestamp(element: PageVisitEvent): Long = element.visit_time
      })
    //      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[PageVisitEvent] {
    //        // 设置延迟时间5s
    //        val delaytime = 5 * 1000L
    //
    //        // 获取当前最大时间
    //        var currentTimestamp: Long = 0L
    //
    //        override def getCurrentWatermark: Watermark = {
    //          val watermark = new Watermark(currentTimestamp - delaytime)
    //          watermark
    //        }
    //
    //        override def extractTimestamp(element: PageVisitEvent, previousElementTimestamp: Long): Long = {
    //          currentTimestamp = Math.max(currentTimestamp, element.visit_time)
    //          currentTimestamp
    //        }
    //      })

    // 对数据做分组并窗口聚合
    val aggStream = dataStream
      .filter(_.method == "GET")
      .filter(data => { // 正则，过滤前端文件
        val pattern = "^((?!\\.(css|js|ico|png|jpg|jpng)$).)*$".r
        pattern.findFirstIn(data.url).nonEmpty
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      // 解决数据乱序延迟问题
      .allowedLateness(Time.minutes(1))
      // 对于延迟超过1分钟的，用侧输出流打印
      .sideOutputLateData(new OutputTag[PageVisitEvent]("late"))
      .aggregate(new HotPageVisitCount(), new HotPageVisitWindowResult())

    // dataStream.print("data")
    // aggStream.print("agg")
    // 10分钟之前的数据会输入到侧输出流
    // aggStream.getSideOutput(new OutputTag[PageVisitEvent]("late"))

    // 统计topN
    val resultDataStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(3))

    // 打印输出
    resultDataStream.print()

    // 执行任务
    env.execute("page visit count")
  }

}

/**
 * 这里声明状态和定时器, 每来一个窗口就注册一个定时器
 */
class TopNHotPages(num: Int) extends KeyedProcessFunction[Long, PageVisitCount, String] {

  //  var hotPageVisitListSate: ListState[PageVisitCount] = _

  var hotPageVisitMapSate: MapState[String, Long] = _

  // 初始化
  override def open(parameters: Configuration): Unit = {
    // 声明一个状态
    // hotPageVisitListSate = getRuntimeContext.getListState[PageVisitCount](new ListStateDescriptor[PageVisitCount]("hotPageListState", classOf[PageVisitCount]))

    hotPageVisitMapSate = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("hotPageMapState", classOf[String], classOf[Long]))
  }

  override def processElement(value: PageVisitCount, ctx: KeyedProcessFunction[Long, PageVisitCount, String]#Context, out: Collector[String]): Unit = {
    hotPageVisitMapSate.put(value.url, value.count)

    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

    // 注册定时器，1分钟之后触发，因为之前设置了延迟1分钟，需要等所有数据来了后才可以清空状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageVisitCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //    val pageVisitListBuffer: ListBuffer[PageVisitCount] = ListBuffer()

    //    val iter = hotPageVisitListSate.get().iterator()
    //    while (iter.hasNext) {
    //      pageVisitListBuffer += iter.next()
    //    }
    //
    //    // 清空状态
    //    hotPageVisitListSate.clear()

    // 判断定时器触发时间(timestamp) == 窗口时间（ctx.getCurrentKey） + 60000 true 则是清空状态的定时器
    if (timestamp == ctx.getCurrentKey + 60000L) {
      hotPageVisitMapSate.clear()
      return
    }

    val pageVisitListBuffer: ListBuffer[(String, Long)] = ListBuffer()

    val iter = hotPageVisitMapSate.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      pageVisitListBuffer += ((entry.getKey, entry.getValue))
    }

    // 排序
    val sortPageVisitCount = pageVisitListBuffer.sortWith(_._2 > _._2).take(num)

    // 将排名格式化成String
    val result = new StringBuffer()
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortPageVisitCount.indices) {
      result.append("排名：").append(i + 1).append("\t")
        .append("页面：").append(sortPageVisitCount(i)._1).append("\t")
        .append("count：").append(sortPageVisitCount(i)._2).append("\n")
    }
    result.append("\n===========================================\n\n")

    TimeUnit.SECONDS.sleep(1)

    out.collect(result.toString)
  }
}

class HotPageVisitCount extends AggregateFunction[PageVisitEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: PageVisitEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class HotPageVisitWindowResult extends org.apache.flink.streaming.api.scala.function.WindowFunction[Long, PageVisitCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageVisitCount]): Unit = {
    out.collect(PageVisitCount(window.getEnd, key, input.iterator.next()))
  }
}


/**
 * apache.log页面数据转换样例类
 *
 * @param ip         ip
 * @param visit_time 访问时间
 * @param url        访问页面
 */
case class PageVisitEvent(ip: String, visit_time: Long, method: String, url: String)


/**
 * apache.log页面数据转换样例类
 *
 * @param ip         ip
 * @param visit_time 访问时间
 * @param url        访问页面
 */
case class PageVisitCount(windowEnd: Long, url: String, count: Long)

