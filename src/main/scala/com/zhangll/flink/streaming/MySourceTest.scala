package com.zhangll.flink.streaming
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ConfigConstants, Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
object MySourceTest {
  def main(args: Array[String]): Unit = {

    // 想要在本地运行打开web界面,调优,需要先在pom文件中添加 flink-runtime-web_2.11
    val conf = new Configuration()

    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    conf.setInteger(RestOptions.PORT, 7080)



//    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createLocalEnvironment(8, conf)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    env.setParallelism(8)
    // 自定以source源获取数据
    val stream1 = env.addSource(new MyJoinSource1)

    stream1.printToErr("从自定义source1接收数据")

    env.execute("my app")
  }
}
