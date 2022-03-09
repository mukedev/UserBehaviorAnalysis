package site.catyy.market_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.Random

/**
 * 分渠道市场推广统计
 *
 * @author zhangYu
 *
 */
object AppMarketByChannel {

  def main(args: Array[String]): Unit = {
    // 构建流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源
    val dataStream = env.addSource(new MySource())
      .assignAscendingTimestamps(_.createTime)

    // 处理和统计数据
    dataStream
      .filter(_.behavior != "uninstall")
      .keyBy(data => (data.channel, data.behavior))
      .timeWindow(Time.days(1), Time.seconds(5))
      .process(new MarketCountByChannel()) // 全窗口函数，代价是需要把数据缓存起来
      .print()

    // 执行任务
    env.execute("market count job")
  }

}

// 定义输入数据样例类
case class MarketBehavior(userId: String, behavior: String, channel: String, createTime: Long)

// 定义输出数据样例类
case class MarketCountResult(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

// 自定义测试数据源
class MySource extends RichSourceFunction[MarketBehavior] {
  // 是否运行标识
  var isRunning = true
  // 定义用户行为和渠道的集合
  val behaviorSet = Seq("view", "click", "download", "install", "uninstall")
  // 定义来源集合
  val channelSet = Seq("appstore", "weibo", "wechat", "tieba")

  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketBehavior]): Unit = {
    // 定义一个数据最大值
    val maxNum = Long.MaxValue
    var count = 0L

    while (isRunning && count < maxNum) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val createTime = System.currentTimeMillis()

      ctx.collect(MarketBehavior(id, behavior, channel, createTime))
      count += 1
      TimeUnit.MILLISECONDS.sleep(50)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

class MarketCountByChannel extends ProcessWindowFunction[MarketBehavior, MarketCountResult, (String, String), TimeWindow] {

  override def process(key: (String, String), context: Context, elements: Iterable[MarketBehavior], out: Collector[MarketCountResult]): Unit = {
    val startStart = new Timestamp(context.window.getStart).toString
    val startEnd = new Timestamp(context.window.getEnd).toString

    out.collect(MarketCountResult(startStart, startEnd, key._1, key._2, elements.size))
  }
}