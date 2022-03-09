package site.catyy.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 基于processFunction处理函数
 *
 * @author zhangYu
 *
 */
object OrderTimeoutWithProcess {

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

    // 数据处理
    val resultDataStream = dataStream
      .keyBy(_.orderId)
      .process(new OrderPayTimeoutResult())

    // 打印输出
    resultDataStream.print("payed")
    resultDataStream.getSideOutput(new OutputTag[OrderResult]("orderPayTimeout")).print("timeout")

    // 执行任务
    env.execute("order pay timeout job")
  }

}

class OrderPayTimeoutResult extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  // 订单创建状态
  var isCreatedState: ValueState[Boolean] = _
  // 订单支付状态
  var isPayedState: ValueState[Boolean] = _
  // 定时器时间状态
  var timerTsState: ValueState[Long] = _

  var orderOrderOutputTag = new OutputTag[OrderResult]("orderPayTimeout")

  override def open(parameters: Configuration): Unit = {
    isCreatedState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isCreated", classOf[Boolean]))
    isPayedState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed", classOf[Boolean]))
    timerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))
  }

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    val isCreated = isCreatedState.value()
    val isPayed = isPayedState.value()
    val timerTs: Long = timerTsState.value()

    // 判断当前事件类型是create还是pay
    if (value.eventType == "create") {
      // 如果已经支付
      if (isPayed) {
        out.collect(OrderResult(value.orderId, "payed successful"))
        // 清空状态
        isCreatedState.clear()
        isPayedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      } else {
        // 如果没有支付
        val ts = value.createTime * 1000L + 15 * 60 * 1000L
        // 更新状态
        isCreatedState.update(true)
        timerTsState.update(ts)
        // 注册定时器
        ctx.timerService().registerEventTimeTimer(ts)
      }
    } else if (value.eventType == "pay") { // 如果当前状态是支付pay，则需要判断create事件是否已到
      if (isCreated) {
        // timerTs是定时器触发时间, 如果小于等于则未超时
        if (value.createTime * 1000L < timerTs) {
          out.collect(OrderResult(value.orderId, "payed successful"))
        } else {
          ctx.output(orderOrderOutputTag, OrderResult(value.orderId, "payed but already timeout!"))
        }
        // 清空状态
        isCreatedState.clear()
        isPayedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      } else {
        // 如果create事件还未到，注册定时器，等到到pay的时间就可以了，然后再在定时器输出一个数据丢失预警
        ctx.timerService().registerEventTimeTimer(value.createTime * 1000L)
        // 更新状态
        isPayedState.update(true)
        timerTsState.update(value.createTime * 1000L)
      }
    }
  }

  // 定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 定时器触发

    // 如果已触发，说明有一个事件没有等到
    if (isPayedState.value()) {
      ctx.output(orderOrderOutputTag, OrderResult(ctx.getCurrentKey, "payed but not found create log"))
    } else {
      ctx.output(orderOrderOutputTag, OrderResult(ctx.getCurrentKey, "order pay timeout"))
    }

    // 清空状态
    isCreatedState.clear()
    isPayedState.clear()
    timerTsState.clear()
  }
}
