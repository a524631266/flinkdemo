package com.zhangll.flink.streaming

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class MyJoinSource1 extends RichParallelSourceFunction[(String, Int)]{
  // 用来控制是否可取消
  var isCancel = true
  //
//  var isCancel2 = AtomicBoolean
  // 这个run是核心的方法,可以用户自己控制产生数据的方式,数据类型,速度v等等
  override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
    val random = new Random()
    val arr = Array("hadoop","linux","spark","flink")
    val size = arr.length
    while (isCancel) {

      val course = arr(random.nextInt(size))
      val score = random.nextInt(10) + 1
      // 发送/生成数据了,可以控制速度 sleep...
      ctx.collect(course,score)
      // 控制产生的速度
      Thread.sleep(500 + random.nextInt(500))
    }
  }

  override def cancel(): Unit = {
    isCancel = false
  }
}
