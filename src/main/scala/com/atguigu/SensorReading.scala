package com.atguigu

case class SensorReading(id: String,  //传感器的id
                         timestamp: Long, //事件时间
                         temperature:Double)  // 温度