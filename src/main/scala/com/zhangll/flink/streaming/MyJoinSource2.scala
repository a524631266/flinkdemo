package com.zhangll.flink.streaming

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class MyJoinSource2 extends RichParallelSourceFunction[String]{
  var isCancel = true
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random = new Random()
    val arr = Array("hadoop","spark","flink","hive")
    val size = arr.length
    while (isCancel) {
      ctx.collect(arr(random.nextInt(size)))
      Thread.sleep(500 + random.nextInt(500))
    }
  }

  override def cancel(): Unit = {
    isCancel = false
  }

}
