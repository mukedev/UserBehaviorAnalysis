package site.catyy.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * 网站总浏览量（PV）的统计
 *
 * @author zhangYu
 *
 */
object PageView {

  def main(args: Array[String]): Unit = {
    // 构建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置时间语义-事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据，从文件中读取
    //    val resource = getClass.getResource("resources目录下的文件")
    //    val fileDataStream = env.readTextFile(resource.getPath).print()
    val fileDataStream = env.readTextFile("/Users/zhangyu/project/github/bigdata/UserBehaviorAnalysis/data/UserBehavior.csv")


    val dataStream = fileDataStream.map(line => {
      val arr = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignAscendingTimestamps(_.createTime * 1000L)

    dataStream
      .filter(_.behavior == "pv")
      //      .map(line => ("pv", 1L)) // 存在数据倾斜问题
      .map(line => (Random.nextString(8), 1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PageViewEvent(), new PageViewWindowResult())
      .keyBy(_.windowEnd)
      .process(new TotalPvCountResult())
      .print()



    // 执行任务
    env.execute("page view job")
  }

}

// 数据输入样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, createTime: Long)

// 数据输出样例类
case class PageViewCount(windowEnd: Long, count: Long)


class PageViewEvent() extends AggregateFunction[(String, Long), Long, Long]() {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PageViewWindowResult extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(window.getEnd, input.head))
  }
}

class TotalPvCountResult extends KeyedProcessFunction[Long, PageViewCount, PageViewCount] {
  var pvValueState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    pvValueState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("pv state", classOf[Long]))
  }

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, PageViewCount]#Context, out: Collector[PageViewCount]): Unit = {

    val currentTotalCount = pvValueState.value()

    pvValueState.update(value.count + currentTotalCount)

    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, PageViewCount]#OnTimerContext, out: Collector[PageViewCount]): Unit = {
    val totalPvCount = pvValueState.value()
    out.collect(PageViewCount(ctx.getCurrentKey, totalPvCount))

    pvValueState.clear()
  }
}