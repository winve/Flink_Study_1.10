package com.itcpay.apitest.tableset

import com.itcpay.model.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
 * 表和流相互转换
 */
object StreamToTable {

  def main(args: Array[String]): Unit = {
    // 1、set up execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2、基于env创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 3、从外部系统读取数据
    val inputStream: DataStream[String] = env.readTextFile("E:\\Idea\\2019\\Flink_Study_1.10\\src\\main\\resources\\sensor.txt")

    // 3.1、map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 4、将 DataStream 转换成表
    val sensorTable1: Table = tableEnv.fromDataStream(dataStream)
    sensorTable1.toAppendStream[(String, Long, Double)].print("table 1")

    // 4.1、数据类型与 schema 对应
    // 4.1.1、基于名称（name-based）
    val sensorTable2: Table = tableEnv.fromDataStream(dataStream, 'timestamp as 'ts, 'id as 'myId, 'temperature)
    sensorTable2.toAppendStream[(Long, String, Double)].print("table 2")

    // 4.1.2、基于位置（position-based）
    val sensorTable3: Table = tableEnv.fromDataStream(dataStream, 'myId, 'ts)
    sensorTable3.toAppendStream[(String, Long)].print("table 3")

    // 5、基于 DataStream 创建临时视图
    tableEnv.createTemporaryView("sensorView1", dataStream)
    tableEnv.createTemporaryView("sensorView2", dataStream, 'id, 'temperature, 'timestamp as 'ts)

    // 6、基于 Table 创建临时视图
    tableEnv.createTemporaryView("sensorView3", sensorTable1)


    env.execute("stream to table start.")
  }

}
