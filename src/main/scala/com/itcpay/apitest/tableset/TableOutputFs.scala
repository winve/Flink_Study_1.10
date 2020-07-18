package com.itcpay.apitest.tableset

import com.itcpay.model.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * 表数据输出到文件中
 */
object TableOutputFs {

  def main(args: Array[String]): Unit = {
    // set up execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // set up table execution environment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 从外部系统读取数据，map成样例类
    val inputStream: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    // 把流转换成表
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature as 'temp, 'timestamp as 'ts)

    // 进行表转换操作
    val resultTable: Table = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    // 将结果表输出到文件
    tableEnv.connect(new FileSystem().path("src/main/resources/output.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
        //          .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")
    resultTable.insertInto("outputTable")

    // TableException: AppendStreamTableSink requires that Table has only insert changes.
    // aggResultTable.insertInto("outputTable")

    env.execute("table output fs start.")
  }

}
