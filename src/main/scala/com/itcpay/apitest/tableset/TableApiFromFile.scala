package com.itcpay.apitest.tableset

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

/**
 * 使用table api从文件中读取数据
 */
object TableApiFromFile {

  def main(args: Array[String]): Unit = {
    // set up execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // set up table environment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2、连接外部系统读取数据
    tableEnv.connect(new FileSystem().path("E:\\Idea\\2019\\Flink_Study_1.10\\src\\main\\resources\\sensor.txt"))
      .withFormat(new OldCsv())
      .withSchema(new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")
    
    // 测试输出
    val inputTable: Table = tableEnv.from("inputTable")
    inputTable.toAppendStream[(String, Long, Double)].print()

    env.execute("table api job test")
  }

}
