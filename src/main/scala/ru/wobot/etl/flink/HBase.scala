package ru.wobot.etl.flink

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import ru.wobot.etl.dto.ProfileDto
import ru.wobot.etl.{JsonUtil, Profile}


object HBase extends App {

  case class Id(url: String, crawlDate: Long)

  val env = ExecutionEnvironment.getExecutionEnvironment
  val toAdd = env.createInput(new IdInputFormat(HBaseConstants.T_PROFILE_TO_ADD))
  val saved = env.createInput(new IdInputFormat(HBaseConstants.T_PROFILE))

  val latest = toAdd.groupBy(x => x.url).sortGroup(x => x.crawlDate, Order.DESCENDING).first(1)
  val toUpdate: DataSet[Id] = latest.leftOuterJoin(saved).where(0).equalTo(0).apply((l: Id, r: Id, collector: Collector[Id]) => {
    if (r == null || r.crawlDate < l.crawlDate)
      collector.collect(l)
  })

  val profileToAdd = env.createInput(new ProfileInputFormat(HBaseConstants.T_PROFILE_TO_ADD))
  val profileToUpdate = toUpdate.join(profileToAdd).where(0, 1).equalTo(0, 1).apply((id: Id, profile: Profile, collector: Collector[Profile]) => {
    collector.collect(Profile(profile.profile.id, profile.crawlDate, profile.profile))
  })

  //profileToUpdate.print()
  profileToUpdate.output(new HBaseOutputFormat(HBaseConstants.T_PROFILE))
  env.execute("Update profiles")

  //  val toAddLength = toAdd.count()
  //  val latestLength = latest.count()
  //  val toUpdateLength = toUpdate.count()
  //  val profileToUpdateLength = profileToUpdate.count()
  //
  //
  //  println(s"toAddLength=$toAddLength")
  //  println(s"latestLength=$latestLength")
  //  println(s"toUpdateLength=$toUpdateLength")
  //  println(s"profileToUpdateLength=$profileToUpdateLength")

  class IdInputFormat(val tableName: String) extends TableInputFormat[Id] {
    override def mapResultToTuple(r: Result): Id = {
      Id(url = Bytes.toString(r.getValue(HBaseConstants.CF_ID, HBaseConstants.C_ID)),
        crawlDate = Bytes.toLong(r.getValue(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE)))
    }

    override def getTableName = tableName

    override def getScanner: Scan = {
      new Scan()
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_ID)
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE)
    }
  }

  class ProfileInputFormat(val tableName: String) extends TableInputFormat[Profile] {
    override def mapResultToTuple(r: Result): Profile = {
      val id = Bytes.toString(r.getValue(HBaseConstants.CF_ID, HBaseConstants.C_ID))
      val crawlDate: Long = Bytes.toLong(r.getValue(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE))
      val json: String = Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("json")))

      Profile(id, crawlDate, JsonUtil.fromJson[ProfileDto](json))
    }

    override def getTableName = tableName

    override def getScanner: Scan = {
      new Scan()
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_ID)
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE)
        .addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"))
    }
  }

}
