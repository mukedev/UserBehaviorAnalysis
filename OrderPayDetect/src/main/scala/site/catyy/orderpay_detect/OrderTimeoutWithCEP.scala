package site.catyy.orderpay_detect

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.util

/**
 * 基于CEP处理数据流
 * @author zhangYu
 *
 */
object OrderTimeoutWithCEP {

  def main(args: Array[String]): Unit = {
    // 构建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置事件语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源，从文件中读取数据
    val fileDataStream = env.readTextFile("/Users/zhangyu/project/github/bigdata/UserBehaviorAnalysis/data/OrderLog.csv")

    // 数据转换
    val dataStream = fileDataStream
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(2)) {
        override def extractTimestamp(element: OrderEvent): Long = element.createTime * 1000L
      })
      .keyBy(_.orderId)

    // 引入CEP处理数据
    // --定义一个模式匹配
    val orderPayPattern = Pattern.begin[OrderEvent]("create").where(_.eventType == "create")
      // 使用宽松近邻
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // --将pattern应用到数据流
    val orderPatternStream: PatternStream[OrderEvent] = CEP.pattern(dataStream, orderPayPattern)

    // 定义侧输出流标签，用于处理超时事件
    val OrderTimeoutOutPutTag = new OutputTag[OrderResult]("orderPayTimeout")

    // 调用select方法，提取并处理正常支付订单和超时支付订单
    val resultDataStream: DataStream[OrderResult] = orderPatternStream.select(OrderTimeoutOutPutTag, new OrderPayTimeoutSelect(), new OrderPaySelect())

    // 打印数据
    resultDataStream.print("payed")
    resultDataStream.getSideOutput(OrderTimeoutOutPutTag).print("timeout")

    // 执行任务
    env.execute("order pay timeout job")

  }

}


// 输入数据样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, createTime: Long)

// 输出数据样例类
case class OrderResult(orderId: Long, resultMsg: String)


// 订单超时事件
class OrderPayTimeoutSelect extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val orderId = map.get("create").iterator().next().orderId
    OrderResult(orderId, s"timeout:\t${l}")
  }
}

// 订单正常事件
class OrderPaySelect extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val orderId = map.get("pay").iterator().next().orderId
    OrderResult(orderId, "pay successful")
  }
}