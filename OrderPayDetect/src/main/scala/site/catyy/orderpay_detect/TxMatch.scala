package site.catyy.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 订单对账
 *
 * @author zhangYu
 *
 */
object TxMatch {

  def main(args: Array[String]): Unit = {
    // 构建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置事件语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源，从文件中读取数据
    val orderLogDataStream = env.readTextFile("/Users/zhangyu/project/github/bigdata/UserBehaviorAnalysis/data/OrderLog.csv")

    // 数据转换
    val orderDataStream = orderLogDataStream
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(2)) {
        override def extractTimestamp(element: OrderEvent): Long = element.createTime * 1000L
      })
      .filter(_.eventType == "pay")
      .keyBy(_.txId)

    // 读取数据源，从文件中读取数据
    val receiptLogDataStream = env.readTextFile("/Users/zhangyu/project/github/bigdata/UserBehaviorAnalysis/data/ReceiptLog.csv")

    // 数据转换
    val receiptDataStream = receiptLogDataStream
      .map(data => {
        val arr = data.split(",")

        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(2)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.createTime * 1000L
      })
      .keyBy(_.txId)

    // 合并两条进行处理
    val resultDataStream = orderDataStream.connect(receiptDataStream)
      .process(new TxPayMatchResult())

    resultDataStream.print()
    resultDataStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-pay")).print("unmatched-pay")
    resultDataStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipt")).print("unmatched-receipt")

    // 任务执行
    env.execute("tx pay match job")
  }

}

// 账单数据样例类
case class ReceiptEvent(txId: String, payChannel: String, createTime: Long)

class TxPayMatchResult extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

  // 定义状态保存订单和账单
  var orderEventState: ValueState[OrderEvent] = _
  var receiptEventState: ValueState[ReceiptEvent] = _

  // 定义两个侧输出流
  val unmatchedOrderEventOutputTag = new OutputTag[OrderEvent]("unmatched-pay")
  val unmatchedReceiptEventOutputTag = new OutputTag[ReceiptEvent]("unmatched-receipt")


  override def open(parameters: Configuration): Unit = {
    orderEventState = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("order state", classOf[OrderEvent]))
    receiptEventState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt state", classOf[ReceiptEvent]))
  }

  override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 判断账单事件是否已到
    val receipt = receiptEventState.value()
    if (receipt != null) {
      out.collect(OrderEvent(value.orderId, value.eventType, value.txId, value.createTime), ReceiptEvent(receipt.txId,receipt.payChannel, receipt.createTime))
      //清空状态
      receiptEventState.clear()
    } else {
      orderEventState.update(value)
      ctx.timerService().registerEventTimeTimer(value.createTime * 1000L + 5000L)
    }
  }

  override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 判断账单事件是否已到
    val order = orderEventState.value()
    if (order != null) {
      out.collect(OrderEvent(order.orderId, order.eventType, order.txId, order.createTime), ReceiptEvent(value.txId,value.payChannel, value.createTime))
      //清空状态
      orderEventState.clear()
    } else {
      receiptEventState.update(value)
      ctx.timerService().registerEventTimeTimer(value.createTime * 1000L + 5000L)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    // 判断触发器触发时，哪个流的事件没有到
    if (orderEventState.value() != null) {
      ctx.output(unmatchedOrderEventOutputTag, orderEventState.value())
    }

    if (receiptEventState.value() != null) {
      ctx.output(unmatchedReceiptEventOutputTag, receiptEventState.value())
    }

  }
}