package ru.wobot.etl.flink

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import ru.wobot.etl.Profile
import ru.wobot.etl._
import ru.wobot.etl.dto.ProfileDto

import org.apache.flink.api.scala._


object Hbase extends App {
  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  val latesesProfiles: DataSet[Profile] = env.createInput(new HbaseInputFormat())
  private val map: DataSet[(String, Long)] = latesesProfiles.map(x=>(x.url, x.crawlDate))

  map.print

  // kick off execution.
  // env.execute

  class HbaseInputFormat extends TableInputFormat[Profile] {
    override def mapResultToTuple(r: Result): Profile = {
      val key: String = Bytes.toString(r.getRow)
      val id: String = Bytes.toString(r.getValue(Bytes.toBytes("id"), Bytes.toBytes("id")))
      val crawlDate: Long = Bytes.toLong(r.getValue(Bytes.toBytes("id"), Bytes.toBytes("crawlDate")))
      val json: String = Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("json")))

      Profile(id, crawlDate, JsonUtil.fromJson[ProfileDto](json))
    }

    override def getTableName: String = "profile"

    override def getScanner: Scan = {
      new Scan()
        .addColumn(Bytes.toBytes("id"), Bytes.toBytes("id"))
        .addColumn(Bytes.toBytes("id"), Bytes.toBytes("crawlDate"))
        .addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"))
    }
  }

}
