package com.zhangll.flink.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FromText2Mysql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("data/dept.txt")
    import org.apache.flink.api.scala._
    val res = text.map(line=>
      {
        val data = line.split("\t")
        val id = data(0).toInt
        val name = data(1)
        val addr = data(2)
        (id, name, addr)
      }
    )
    res.addSink(new Sink2Mysql)

    env.execute("mysql sink")
  }
}
