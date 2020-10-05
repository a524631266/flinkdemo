package com.zhangll.flink.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class Sink2Mysql extends RichSinkFunction[(Int,String,String)]{
  @transient var ps: PreparedStatement = _
  @transient var conn: Connection = _

  /**
    * 初始化一个新的参数
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {

    super.open(parameters)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop01:3306/flink"
    val user = "root"
    val passWd = "26Hang0918."
    // 类加载器
    Class.forName(driver)
    conn = DriverManager.getConnection(url, user, passWd)
    ps = conn.prepareStatement("insert into dept(id, name, addr) values (?,?,?)")
  }

  /**
    * 每次插入数据都调用invoke
    * @param value
    * @param context
    */
  override def invoke(value: (Int, String, String), context: SinkFunction.Context[_]): Unit = {
//    super.invoke(value, context)
    try {
      ps.setInt(1,value._1)
      ps.setString(2,value._2)
      ps.setString(3,value._3)
      ps.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  override def close(): Unit = {
    super.close()
    if(ps!=null) {
      ps.close()
    }
    if(conn!=null) {
      conn.close()
    }
  }
}
