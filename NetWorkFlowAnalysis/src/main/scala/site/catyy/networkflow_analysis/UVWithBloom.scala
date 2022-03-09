package site.catyy.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * 利用bloom过滤器和redis统计unique visitor
 *
 * @author zhangYu
 *
 */
object UVWithBloom {

  def main(args: Array[String]): Unit = {
    // 构建流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源
    val fileDataStream = env.readTextFile("/Users/zhangyu/project/github/bigdata/UserBehaviorAnalysis/data/UserBehavior.csv")

    val dataStream = fileDataStream
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.createTime * 1000L)

    // 处理数据
    dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) // 自定义触发器
      .process(new UVCountWithBloom())


    env.execute("uv job")
  }

}

// 触发器，每来一条数据，直接触发窗口计算并清空窗口状态
class MyTrigger extends Trigger[(String, Long), TimeWindow] {

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult
  = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  // 有waterMark改变时的操作
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

  }
}

// 自定义一个布隆过滤器，主要是一个位图和hash函数
class Bloom(size: Long) extends Serializable {
  private val cap = size

  // hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0

    for (i <- 0 until value.length)
      result = result * seed + value.charAt(i)

    (cap - 1) & result
  }
}

class UVCountWithBloom extends ProcessWindowFunction[(String, Long), UVCount, String, TimeWindow] {

  // 定义redis连接器和布隆过滤器
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloomFilter = new Bloom(1 << 29) // 位的个数：2^6(64) * 2^20(1M) * 2^3(8bit) = 64M

  // 本来是收集所有数据，才会触发窗口计算，现在是每一条都会触发计算
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UVCount]): Unit = {
    // 定义redis中的存储位图的key
    val storedBitMapKey = context.window.getEnd.toString

    // 另外将当前窗口的uv count值，作为状态保存到redis里，用一个叫做uvcount的hash表来保存（windowEnd, count）
    val uvCountMap = "uvcount"
    val currentKey = context.window.getEnd.toString

    var count = 0L

    // 从redis中取出当前窗口的uv count值
    if (jedis.hget(uvCountMap, currentKey) != null) {
      count = jedis.hget(uvCountMap, currentKey).toLong
    }

    // 去重，判断当前userid在hash值中的位置是否为0
    val userId = elements.last._2.toString
    // 计算hash值
    val offset = bloomFilter.hash(userId, 61)
    // 用redis的位操作命令，取bitmap中对应位的值
    val isExist = jedis.getbit(storedBitMapKey, offset)

    if (!isExist) {
      // 如果不存在，那么对应位图的位置1，并且count的值加1
      jedis.setbit(storedBitMapKey, offset, true) // true表示写1
      jedis.hset(uvCountMap, currentKey, (count + 1).toString)
    }
  }
}