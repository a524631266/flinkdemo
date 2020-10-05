package com.zhangll.flink.streaming

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    // 获取执行环境/创建执行上下文对象
    // Configuration
    // 正对无边界流
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    // dataset
//    val env = ExecutionEnvironment.getExecutionEnvironment
    // 第二步 获取源
    // 数据源的问题就会自动停止
    val text: DataStream[String] = env.readTextFile("hdfs://hadoop01:8020/test.tsv")

    // 第三步: 指定数据的转换操作

    val data = text.map(_.split("\t")(1)).flatMap(_.toList)


    // 第四步保存计算结果的保存位置
//    data.writeAsText("data/abc")
    data.print()

    env.execute("my firest flink demo print")

  }
}
