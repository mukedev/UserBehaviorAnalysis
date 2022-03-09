package site.catyy.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 *
 * @author zhangYu
 *
 */
object LoginFail {

  def main(args: Array[String]): Unit = {
    // 构建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据
    val fileDataStream = env.readTextFile("/Users/zhangyu/project/github/bigdata/UserBehaviorAnalysis/data/LoginLog.csv")

    // 数据转换
    val dataStream: DataStream[LoginLog] = fileDataStream
      .map(line => {
        val arr = line.split(",")
        LoginLog(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginLog](Time.seconds(3)) {
        override def extractTimestamp(element: LoginLog): Long = element.createTime
      })

    // 判断和检测2s之内是否连续登录失败
    dataStream
      .keyBy(_.userId)
      .process(new LoginFailWarningResult(2))
      .print()


    // 执行任务
    env.execute("login fail detect job")
  }

}

// 输入的登录数据样例类
case class LoginLog(userId: Long, ip: String, eventType: String, createTime: Long)

// 输出报警信息样例类
case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, warnMsg: String)

class LoginFailWarningResult(failNum: Int) extends KeyedProcessFunction[Long, LoginLog, LoginFailWarning] {
  // 定义状态，保存当前所有的登录失败的事件，保存定时器的事件戳
  var loginFailListState: ListState[LoginLog] = _
  var timerTsState: ValueState[Long] = _


  // 初始环境
  override def open(parameters: Configuration): Unit = {
    loginFailListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginLog]("login fail state", classOf[LoginLog]))
    timerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("fail onTime", classOf[Long]))
  }

  // 定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginLog, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    // 判断是否达到预警上限
    val listBuffer: ListBuffer[LoginLog] = ListBuffer()
    val iter = loginFailListState.get().iterator()

    while (iter.hasNext) {
      listBuffer += iter.next()
    }
    if (listBuffer.size >= failNum) {
      out.collect(LoginFailWarning(listBuffer.head.userId, listBuffer.head.createTime, listBuffer.last.createTime, s"2s内登录失败次数：${listBuffer.size}次"))
    }

    loginFailListState.clear()
    timerTsState.clear()
  }

  override def processElement(value: LoginLog, ctx: KeyedProcessFunction[Long, LoginLog, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 判断当前登录事件是否是失败
    if (value.eventType == "fail") {
      loginFailListState.add(value)

      // 判断是否有注册定时器，如果没有则注册定时器，设置触发时间为2s后
      if (timerTsState.value() == 0) {
        val ts = value.createTime * 1000L + 2000L
        timerTsState.update(ts)
        ctx.timerService().registerEventTimeTimer(ts)
      }
    } else {
      // 如果成功，则清空状态和定时器
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()
    }


  }

}