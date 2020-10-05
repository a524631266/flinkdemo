package com.zhangll.flink.streaming

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class IntSource extends RichParallelSourceFunction[Int]{
  var isCannel = true
  override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
    var count= 1;
    while (isCannel) {
      count += 1
      ctx.collect(count)
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {
    isCannel = false
  }
}
