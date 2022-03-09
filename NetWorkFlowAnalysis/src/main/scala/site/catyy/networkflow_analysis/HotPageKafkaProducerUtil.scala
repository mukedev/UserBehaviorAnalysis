package site.catyy.networkflow_analysis

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.io.BufferedSource

/**
 * 需求：
 * 从文件中读取数据，推送到kafka 的 hotItems
 *
 * @author zhangYu
 *
 */
object HotPageKafkaProducerUtil {

  def writeData(topic: String): Unit = {
    // kafka的配置
    val props = new Properties()
    props.setProperty("bootstrap.servers", "server10:9092,server11:9092,server12:9092")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // 读取文件
    val bufferedSource: BufferedSource = io.Source.fromFile("/Users/zhangyu/project/github/bigdata/UserBehaviorAnalysis/data/apache.log")

    if (bufferedSource != null) {
      for (line <- bufferedSource.getLines()) {
        val record = new ProducerRecord[String, String](topic, line)

        producer.send(record)
      }
    }

    producer.close()
  }


  def main(args: Array[String]): Unit = {
    writeData("page_visit")
    Thread.sleep(50)
  }
}
