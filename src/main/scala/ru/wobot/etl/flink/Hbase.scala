package ru.wobot.etl.flink

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import ru.wobot.etl.Profile


object Hbase extends App {

  case class Id(url: String, crawlDate: Long)

  val env = ExecutionEnvironment.getExecutionEnvironment
  val profileToAdd = env.createInput(new IdInputFormat())
  val toAddLength = profileToAdd.count()
  private val latest = profileToAdd.groupBy(x => x.url).sortGroup(x => x.crawlDate, Order.DESCENDING).first(1)
  private val latestLength = latest.count()

  latest.print()
  println(s"toAddLength=$toAddLength")
  println(s"latestLength=$latestLength")

  class IdInputFormat extends TableInputFormat[Id] {
    override def mapResultToTuple(r: Result): Id = {
      Id(Bytes.toString(r.getValue(HbaseConstants.CF_ID, HbaseConstants.C_ID)),
        Bytes.toLong(r.getValue(HbaseConstants.CF_ID, HbaseConstants.C_CRAWL_DATE)))
    }

    override def getTableName = HbaseConstants.T_PROFILE_TO_ADD

    override def getScanner: Scan = {
      new Scan()
        .addColumn(HbaseConstants.CF_ID, HbaseConstants.C_ID)
        .addColumn(HbaseConstants.CF_ID, HbaseConstants.C_CRAWL_DATE)
    }
  }

  class ProfileInputFormat extends TableInputFormat[Profile] {
    override def mapResultToTuple(r: Result): Profile = {
      val key = Bytes.toString(r.getRow)
      val id = Bytes.toString(r.getValue(HbaseConstants.CF_ID, HbaseConstants.C_ID))
      val crawlDate: Long = Bytes.toLong(r.getValue(HbaseConstants.CF_ID, HbaseConstants.C_CRAWL_DATE))
      //val json: String = Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("json")))

      Profile(id, crawlDate, null)
      //Profile(id, crawlDate, JsonUtil.fromJson[ProfileDto](json))
    }

    override def getTableName = HbaseConstants.T_PROFILE

    override def getScanner: Scan = {
      new Scan()
        .addColumn(HbaseConstants.CF_ID, HbaseConstants.C_ID)
        .addColumn(HbaseConstants.CF_ID, HbaseConstants.C_CRAWL_DATE)
      //.addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"))
    }
  }

}
