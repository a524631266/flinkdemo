package com.zhangll.flink.streaming

import org.apache.flink.streaming.api.scala.{JoinedStreams, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows, TumblingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object TransformationFunc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(8)
    val input = env.readTextFile("data/wc.txt")

    val word = input.flatMap(line => line.split(" "))
    val filteredWord = word.filter(_.nonEmpty)
      .map((_,1))
        .keyBy(0)
        .sum(1)
//      .printToErr()
    val data = env.fromCollection(Range(1,10))

    data
      .map((1,_))
        .keyBy(0)
//        .max(1)
      .maxBy(1)
//        .printToErr()

    val source1 = env.fromElements(("hadoop",7),("hive",5),("spark",6))
    val source2 = env.fromElements("hadoop","spark")
    // 合并流用来合并管道流
    source1.union(source2.map((_,1)))
//      .printToErr()


    val tmp = source2.map((1,_))
    val d = source1.join(tmp)
        // 左表的a.id
        .where(_._1)
        // 右表的b.id
        .equalTo(_._2)
//      .window(TumblingTimeWindows.of(Time.seconds(3)))
        .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//    .window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
        .apply((a,b) => (a._1,a._2,b._1,b._2))
//      .assignAscendingTimestamps(TimeCharacteristic.ProcessingTime)
//      .assignAscendingTimestamps(_)
      .printToErr()

    env.execute("aa")

  }
}
