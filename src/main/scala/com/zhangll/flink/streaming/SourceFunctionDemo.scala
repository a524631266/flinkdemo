package com.zhangll.flink.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
object SourceFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val abc = env.readTextFile("data/abc")
    val input1 = env.socketTextStream("hadoop01", 9998)
    input1.print()
    val input2 = env.fromCollection(Range(1, 10))
//    abc.print()
    input2.filter(_ % 2 == 0 ).printToErr()

    val data = Array("hello","spark","hello")
    val input3 = env.fromCollection(data)
    input3.keyBy(a => a).map(a=> (a,Thread.currentThread().getId)).printToErr() // 一个组在同一个slot中获取

    val input4 = env.fromElements(1,2,3,4,65,76)
    input4.printToErr()
    env.execute("my second ")
  }
}
