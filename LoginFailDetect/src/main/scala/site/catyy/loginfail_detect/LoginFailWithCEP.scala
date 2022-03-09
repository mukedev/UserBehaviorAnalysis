package site.catyy.loginfail_detect

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.util

/**
 *
 * @author zhangYu
 *
 */
object LoginFailWithCEP {

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

    // 定义CEP 匹配模式(2s之内连续登录失败2次。CEP内部会处理乱序问题)
//    val loginFailPattern: Pattern[LoginLog, LoginLog] = Pattern.begin[LoginLog]("firstFail").where(_.eventType == "fail")
//      .next("secondFail").where(_.eventType == "fail")
//      .within(Time.seconds(2))

    // 定义CEP 匹配模式(5s之内连续登录失败3次。CEP内部会处理乱序问题)
    val loginFailPattern: Pattern[LoginLog, LoginLog] = Pattern.begin[LoginLog]("firstFail").where(_.eventType == "fail")
      .next("secondFail").where(_.eventType == "fail")
      .next("thirdFail").where(_.eventType == "fail")
      .within(Time.seconds(5))

    // 将匹配模式应用到数据流上,需要将数据流分组
    val patternStream: PatternStream[LoginLog] = CEP.pattern(dataStream.keyBy(_.userId), loginFailPattern)

    // 检出符合模式的数据流，需要调用select
    val resultDataStream: DataStream[LoginFailWarning] = patternStream.select(new LoginFailEventMatch())

    // 打印输出
    resultDataStream.print()

    // 执行任务
    env.execute("login fail by CEP Job")


  }

}

class LoginFailEventMatch extends PatternSelectFunction[LoginLog, LoginFailWarning] {
  override def select(map: util.Map[String, util.List[LoginLog]]): LoginFailWarning = {
    val firstFailEvent = map.get("firstFail").get(0)
    val thirdFailEvent = map.get("thirdFail").iterator().next()

    LoginFailWarning(firstFailEvent.userId, firstFailEvent.createTime, thirdFailEvent.createTime, "检测出2秒内失败2次")
  }
}
