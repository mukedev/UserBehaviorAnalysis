package site.catyy.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 订单对账
 * @author zhangYu
 *
 */
object TxMatchWithJoin {

  def main(args: Array[String]): Unit = {
    // 构建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置事件语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源，从文件中读取数据
    val orderLogDataStream = env.readTextFile("/Users/zhangyu/project/github/bigdata/UserBehaviorAnalysis/data/OrderLog.csv")

    // 数据转换
    val orderDataStream: KeyedStream[OrderEvent, String] = orderLogDataStream
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
    val receiptDataStream: KeyedStream[ReceiptEvent, String] = receiptLogDataStream
      .map(data => {
        val arr = data.split(",")

        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(2)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.createTime * 1000L
      })
      .keyBy(_.txId)

    // interval join操作
    val resultDataStream = orderDataStream.intervalJoin(receiptDataStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new TxMatchWithJoinResult())

    resultDataStream.print()

    // 执行任务
    env.execute("tx match join")
  }

}

class TxMatchWithJoinResult extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect(left, right)
    // todo 这里不能把不匹配的数据输出，这个业务更适合使用connectFunction，所以不同业务场景需要用不同的方法
    // todo 这里匹配的数据少是因为数据从文件中一次性加载到流环境，会存在数据跳变问题，在真实的流环境下不会有这个问题
  }
}