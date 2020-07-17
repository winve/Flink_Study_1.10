package com.itcpay.apitest.tableset

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * 表的查询与转换
 */
object TableApiOperation {

  def main(args: Array[String]): Unit = {
    // set up execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 基于env创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 连接外部系统读取数据（文件）
    tableEnv.connect(new FileSystem().path("src/main/resources/sensor.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    // 3、表的查询转换
    val sensorTable: Table = tableEnv.from("inputTable")

    // 3.1、简单查询
    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    // 3.2、聚合转换
    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

    // 3.3、SQL
    val aggResultSqlTable: Table = tableEnv.sqlQuery("select id, count(id) as cnt from inputTable group by id")

    // 测试输出
    resultTable.toAppendStream[(String, Double)].print("filter")
    aggResultTable.toRetractStream[(String, Long)].print("agg result")  // 回收流
    aggResultSqlTable.toRetractStream[(String, Long)].print("agg sql")

    env.execute("table api start.")
  }

}
