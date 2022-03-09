package site.catyy.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 *
 * @author zhangYu
 *
 */
object UniqueVisitor {

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
      .timeWindowAll(Time.hours(1)) // 不分组，基于dataStream开1小时滚动窗口
      .apply(new UVCountResult())
      .print()

    env.execute("uv job")
  }

}

// 定义一个全窗口函数的继承类，通过set去重特性实现功能（适合访问量百万下的情况，如果更高可以用redis+布隆过滤器）
class UVCountResult extends AllWindowFunction[UserBehavior, UVCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UVCount]): Unit = {
    // 定义一个set去重
    var userSet: Set[Long] = Set[Long]()

    for(userBehavior <- input) {
      userSet += userBehavior.userId
    }

    out.collect(UVCount(window.getEnd, userSet.size))
  }
}


// 数据输出样例类
case class UVCount(windowEnd: Long, count: Long)