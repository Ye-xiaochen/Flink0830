package com.atguigu

import org.apache.flink.streaming.api.scala._

object ReduceExample{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //scala 列表并接操作
    val l1 =List("a")
    val l2 =List("b")
    println(l1 ::: l2)

    val inputStream: DataStream[(String,List[String])] = env.fromElements(
      ("en",List("tea")), ("fr", List("vin")),("en", List("cake")),("fr",List("je")))
    inputStream
      .keyBy(0)
      .reduce((x,y) =>(x._1,x._2 ::: y._2))
      .print()

    env.execute()
  }
}