package site.catyy.loginfail_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 *
 * @author zhangYu
 *
 */
object LoginFailAdvance {

  def main(args: Array[String]): Unit = {
    // 构建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据
    val fileDataStream = env.readTextFile("/Users/zhangyu/project/github/bigdata/UserBehaviorAnalysis/data/LoginLog.csv")

    // 数据转换
    val dataStream: DataStream[LoginAdvanceLog] = fileDataStream
      .map(line => {
        val arr = line.split(",")
        LoginAdvanceLog(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginAdvanceLog](Time.seconds(3)) {
        override def extractTimestamp(element: LoginAdvanceLog): Long = element.createTime
      })

    // 判断和检测2s之内是否连续登录失败
    dataStream
      .keyBy(_.userId)
      .process(new LoginFailAdvanceWarningResult(2))
      .print()

    // 执行任务
    env.execute("login fail advance job")
  }
}

// 输入的登录数据样例类
case class LoginAdvanceLog(userId: Long, ip: String, eventType: String, createTime: Long)

// 输出报警信息样例类
case class LoginFailAdvanceWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, warnMsg: String)

class LoginFailAdvanceWarningResult(failNum: Int) extends KeyedProcessFunction[Long, LoginAdvanceLog, LoginFailAdvanceWarning] {
  // 定义状态，保存当前所有的登录失败的事件，保存定时器的事件戳
  var loginFailValueState: ValueState[LoginAdvanceLog] = _
  var timerTsState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    loginFailValueState = getRuntimeContext.getState(new ValueStateDescriptor[LoginAdvanceLog]("login fail advance state", classOf[LoginAdvanceLog]))
    timerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("fail onTime", classOf[Long]))
  }

  override def processElement(value: LoginAdvanceLog, ctx: KeyedProcessFunction[Long, LoginAdvanceLog, LoginFailAdvanceWarning]#Context, out: Collector[LoginFailAdvanceWarning]): Unit = {
    // 判断事件的类型是否是失败
    if (value.eventType == "fail") {
      // 如果状态中已经有失败事件，则用当前事件的事件与状态中的事件时间进行比较
      if (loginFailValueState.value() != null) {
        // 如果在2s之内直接输出结果
        if (value.createTime - loginFailValueState.value().createTime <= 2) {
          // 在2s内，输出预警并清空状态
          // todo 这里有个问题，就是一个用户会输出多次，如果事件是乱序，处理起来就更麻烦！好的解决方案是使用flink CEP来处理
          out.collect(LoginFailAdvanceWarning(value.userId, loginFailValueState.value().createTime, value.createTime, "2s内连续失败大于等于2次"))
        }
      }
      // 大于2s则用当前的事件替换原先的事件
      // 状态为空，则直接将当前事件更新到状态
      loginFailValueState.update(value)
    } else {
      // 如果是成功，则直接清空状态
      loginFailValueState.clear()
    }
  }

}