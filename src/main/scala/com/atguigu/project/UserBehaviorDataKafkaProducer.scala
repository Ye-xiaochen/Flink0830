package com.atguigu.project

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object UserBehaviorDataKafkaProducer {
  def main(args: Array[String]): Unit = {
    writerToKafka("hotitems")
  }

  def writerToKafka(topic: String): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)
    val bufferedSource = io.Source.fromFile("C:\\Users\\叶孝晨\\Desktop\\flink\\2.资料/UserBehavior.csv")
    for (line <- bufferedSource.getLines()){
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
