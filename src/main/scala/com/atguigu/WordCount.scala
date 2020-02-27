package com.atguigu

// 导入了常用的隐式类型转换，所有的flink程序必须导入
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
object WordCount {
  case class WordWithCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {
    // sparkContext
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // centos : yum install nc
    val text = env.socketTextStream("hadoop102",9999,'\n')

      val wordCountStream =text
        //按照空格来切割输入的字符串
        //flatMap输入的是列表，返回的0、1或者多个元素的列表
        .flatMap{w => w.split("\\s")}
        // mapreduce中的map这一步
        .map{w => WordWithCount(w,1)}
        .keyBy("word")
        //滚动窗口5s
        .timeWindow(Time.seconds(5))
        .sum("count")

    wordCountStream.print()

    env.execute()
  }
}
