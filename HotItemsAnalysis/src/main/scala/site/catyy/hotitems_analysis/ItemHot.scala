package site.catyy.hotitems_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random


/**
 * 需求1：实时热门商品统计
 * @author zhangYu
 *
 */
object ItemHot {
  def main(args: Array[String]): Unit = {

    // 读取流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 临时设置并行度为1（生产环境不会这样做）
    env.setParallelism(1)
    // 设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源，从文件中读取
    val sourceStream = env.readTextFile("/Users/zhangyu/project/github/bigdata/UserBehaviorAnalysis/data/UserBehavior.csv")

    sourceStream.print()

    // 自定义商品点击数据
    //        val sourceStream = env.addSource(new UserBehaviorSource())

    // 从kafka消费数据
    val props = new Properties()
    props.setProperty("bootstrap.servers", "server10:9092,server11:9092,server12:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    //    val sourceStream = env.addSource(new FlinkKafkaConsumer010[String]("hotItems", new SimpleStringSchema(), props))

    val dataStream = sourceStream.map(line => {
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


    // 窗口聚合
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") // 过滤pv行为
      .keyBy("itemId") // 根据商品id分组
      .timeWindow(Time.hours(1), Time.minutes(5)) // 窗口大小1小时，5分钟滑动一次
      .aggregate(new CountAgg(), new ItemViewWindowResult())


    val resultDataStream: DataStream[String] = aggStream
      .keyBy("windowEnd")
      .process(new TopNHotItems(5))

    resultDataStream.print()

    env.execute("hot item")
  }

}


/**
 * IN：输入
 * ACC：中间结果
 * OUT: 输出结果
 */
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


class ItemViewWindowResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0 // 因为元组中只有一个元素，则可以直接指定，Tuple1是java的类型
    val windowEnd: Long = window.getEnd
    val count = input.iterator.next() // 迭代器中只一个输出结果，所以可以直接取

    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}


class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  // 注册一个ListState状态
  private var itemViewCountListState: ListState[ItemViewCount] = _


  // open只会执行一次
  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState[ItemViewCount](new ListStateDescriptor[ItemViewCount]("hotItem", classOf[ItemViewCount]))
  }


  // 处理每一条数据
  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    itemViewCountListState.add(value)

    // 注册一个定时器，窗口数据收集完后+1s执行
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器执行函数
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 定义一个集合，用来完成排序功能
    val listBuffer: ListBuffer[ItemViewCount] = ListBuffer()

    // 当定时器执行的时候，说明窗口数据都已经收集
    val iter = itemViewCountListState.get().iterator()

    // while循环遍历
    while (iter.hasNext) {
      listBuffer += iter.next()
    }

    // 此时可以清空状态
    itemViewCountListState.clear()

    // 排序,作倒序，这里实用柯里化方法
    val sortResult: mutable.Seq[ItemViewCount] = listBuffer.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将排名格式化成String
    val result = new StringBuffer()
    println("窗口结束时间戳---" + timestamp)
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortResult.indices) {
      result.append("排名：").append(i + 1).append("\t")
        .append("商品：").append(sortResult(i).itemId).append("\t")
        .append("count：").append(sortResult(i).count).append("\n")
    }
    result.append("\n===========================================\n\n")

    TimeUnit.SECONDS.sleep(1)

    out.collect(result.toString)
  }
}

/**
 * 生成 UserBehavior 商品点击数据
 */
class UserBehaviorSource extends SourceFunction[String] {
  var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    // 设置生成记录数据最大条数
    val maxValue = Long.MaxValue
    var count = 0L

    while (isRunning && count <= maxValue) {
      // userId
      val userId: Long = Random.nextInt(100000) + 100000
      // itemId
      val itemId: Long = Random.nextInt(100) + 100
      // categoryId
      val categoryId: Int = Random.nextInt(100000) + 100000
      // behavior
      val behaviorSeq: Seq[String] = Seq("pv", "cat")
      val behavior: String = behaviorSeq(Random.nextInt(behaviorSeq.size))
      // timestamp
      val timestamp = System.currentTimeMillis() / 1000L

      sourceContext.collect(s"${userId},${itemId},${categoryId},${behavior},${timestamp}")

      count += 1
      // 等待50ms生成一条数据
      TimeUnit.MILLISECONDS.sleep(50L)
    }
  }

  override def cancel(): Unit = isRunning = false
}


// 数据输入样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, createTime: Long)

// 数据输出样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
