package com.zhangll.flink.streaming

import org.apache.flink.configuration.{ConfigConstants, Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object TwoSourceJoin {
  def main(args: Array[String]): Unit = {

    // 想要在本地运行打开web界面,调优,需要先在pom文件中添加 flink-runtime-web_2.11
    val conf = new Configuration()

    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    conf.setInteger(RestOptions.PORT, 7080)



    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createLocalEnvironment(2, conf)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //    env.setParallelism(8)
    // 自定以source源获取数据
    val stream1: DataStream[(String, Int)] = env.addSource(new MyJoinSource1)
    val right: DataStream[String] = env.addSource(new MyJoinSource2)

    stream1.join(right)
        .where(_._1)
        .equalTo(_.toString)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
        .apply((a,b) => (a._1,a._2,b))
//        .printToErr()

    val connct = stream1.connect(right)
//    stream1.printToErr("从自定义source1接收数据")
    connct.map(a => {
//      println("left::",a)
    }, b=>{
//      println("right::",b)
    })
//      .printToErr("function")


    connct.map(new CoMapFunction[(String,Int),(String),(String)]{
      override def map1(value: (String, Int)): String = {
        value._1
      }

      override def map2(value: String): String = {
        value
      }
    })
//        .printToErr("CoMapFunction")
    // 配置流
    connct.flatMap(new CoFlatMapFunction[(String,Int),(String),(String,Int)] {
      var conf: String = _

      // 可以把第一个作为实时流,这里的业务逻辑就是要把配置流中的数据与实时流的字符相等的额时候
      override def flatMap1(value: (String, Int), out: Collector[(String,Int)]): Unit = {
        if(value._1 == conf) {
          out.collect(value)
        }
      }


      // 可以把第二个作为配置流 ,此时可以不用out,因为只是根据上方的实际过程使用
      override def flatMap2(value: String, out: Collector[(String,Int)]): Unit = {
          conf = value
      }
    })
//        .printToErr("配置流的处理")

    val intSource = env.addSource(new IntSource)
    intSource.split(
      t=>{
        t % 3 match {
          case 0 => List("r0")
          case 1 => List("r1")
          case 2 => List("r2")
        }
      }
    ).select("r0")
        .printToErr("r0")

    env.execute("my app")
  }
}
