package com.atguigu.project

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object FlinkKafkaConsumerUserBehavior{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop:103:9092,hadoop104:9092")
    props.setProperty("group.id","consumer-group")
    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

/*    val stream = env
      .addSource(
        new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), props)
      )
      .map(r => {
        val arr: Array[String] = r.split(",")
        UserBehavior(arr(0).toLong,arr(1).toLong,arr(3).toInt,arr(4),arr(5).toLong * 1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.itemId)
      .timeWindow(Time.minutes(60),Time.minutes(5))
      .aggregate(new CountAgg, new WindowResultFunction)

    stream.print()
    env.execute()*/
  }

//  class U
}
