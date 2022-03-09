package site.catyy.market_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 *
 * @author zhangYu
 *
 */
object AdClickAnalysis {

  def main(args: Array[String]): Unit = {
    // 构建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 定义时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源
    val resource = getClass.getResource("/AdClickLog.csv")
    val fileDataStream: DataStream[String] = env.readTextFile(resource.getPath)

    // 处理和统计窗口数据
    val dataStream: DataStream[AdClickLog] = fileDataStream
      .map(line => {
        val arr = line.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.createTime * 1000L)

    // 新增一个功能，实现黑名单报警，将异常点击输出到侧输出流
    val filterBlackListDataStream = dataStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUserResult(100))


    dataStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdClickCount(), new AdClickResultByWindow())
      .print("count out")

    filterBlackListDataStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("warning")
    // 执行任务
    env.execute("adClick job")
  }

}

// 定义输入数据样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, createTime: Long)

// 定义输出数据样例类
case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)

// 侧输出流-黑名单
case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

class AdClickCount extends AggregateFunction[AdClickLog, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdClickResultByWindow extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    out.collect(AdClickCountByProvince(new Timestamp(window.getEnd).toString, key, input.head))
  }
}

class FilterBlackListUserResult(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
  // 定义用户的点击量的保存状态
  var countState: ValueState[Long] = _

  // 每天0点清空状态，只监测一天的量是否达到访问上线
  var resetTimerTsState: ValueState[Long] = _

  // 判断是否已经在黑名单，如果存在就不加入
  var isBlackState: ValueState[Boolean] = _

  override def open(parameters: Configuration): Unit = {
    countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ad black count state", classOf[Long]))
    resetTimerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ad black reset timer state", classOf[Long]))
    isBlackState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ad blackList isBlack state", classOf[Boolean]))
  }

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    // 当前的count
    val curCount = countState.value()

    // 如果第一个数据来了，就定义清空时间状态，并注册定时器
    if (curCount == 0) {
      val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - (8 * 60 * 60 * 1000) //表示明天

      resetTimerTsState.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    if (curCount >= maxCount) {
      // 判断是否存在黑名单中，不存在则输出到侧输出流
      if (!isBlackState.value()) {
        isBlackState.update(true)
        ctx.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(value.userId, value.adId, s"当天点击超过 ${maxCount} 次"))
      }
      return
    }

    // 如果没有达到上限
    countState.update(curCount + 1)
    out.collect(value)
  }
}
