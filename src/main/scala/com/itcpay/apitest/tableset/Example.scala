package com.itcpay.apitest.tableset

import com.itcpay.model.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object Example {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 从文件中读取数据
    val inputStream: DataStream[String] = env.readTextFile("E:\\Idea\\2019\\Flink_Study_1.10\\src\\main\\resources\\sensor.txt")

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 1、基于env创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2、基于tableEnv，将流转换成表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 3、转换操作，得到提取结果
    // 3.1、调用table api，做转换操作
    val resultTable: Table = dataTable.select("id, temperature")
      .filter("id == 'sensor_1'")

    // 3.2、写SQL实现转换
    tableEnv.createTemporaryView("dataTable", dataTable)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from dataTable
        |where id = 'sensor_1'
        |""".stripMargin)


    // 4、把表转换成流打印输出
    val resultStream: DataStream[(String, Double)] = resultSqlTable.toAppendStream[(String, Double)]
    resultStream.print()

    env.execute("table api example job")
  }

}
