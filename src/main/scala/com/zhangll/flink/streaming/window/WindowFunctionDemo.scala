package com.zhangll.flink.streaming.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowFunctionDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.socketTextStream("hadoop01", port = 9999)

    import org.apache.flink.api.scala._
    // 按照窗口进行计数,每个单词出现的次数
    val s = source.flatMap(
      _.split(" ")
    )
      .filter(_.nonEmpty)
      .map((_,1))
    // : KeyedStream[(String, Int), Tuple]
      .keyBy(0)
      // 没有数据就不会触发窗口
      //.timeWindow(Time.seconds(5))
      .timeWindow(Time.seconds(5),Time.seconds(3))
      .sum(1)
      .printToErr("5 秒的滚动窗口")

    env.execute("开始执行")
  }
}
