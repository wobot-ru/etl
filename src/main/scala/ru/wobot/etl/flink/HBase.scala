package ru.wobot.etl.flink

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import ru.wobot.etl.Profile
import ru.wobot.etl.flink.OutputFormat.HBaseOutputFormat


object HBase extends App {

  val env = ExecutionEnvironment.getExecutionEnvironment
  val profileToProcess = env.createInput(InputFormat.profileToProcess)
  val profiles = env.createInput(InputFormat.profilesStore)

  updateProfileView(env, profileToProcess, profiles, OutputFormat.profilesStore)

  def updateProfileView(env: ExecutionEnvironment, processing: DataSet[Profile], saved: DataSet[Profile], profilesStore: HBaseOutputFormat[Profile]) = {
    val latest = processing.groupBy(x => x.url).sortGroup(x => x.crawlDate, Order.DESCENDING).first(1)
    val toUpdate = latest.leftOuterJoin(saved).where(0).equalTo(0).apply((l: Profile, r: Profile, collector: Collector[Profile]) => {
      if (r == null || r.crawlDate < l.crawlDate)
        collector.collect(l)
    })

    toUpdate.output(OutputFormat profilesStore)
    env.execute("Update profiles")

    val admin = new HBaseAdmin(HBaseConfiguration.create)
    admin.disableTable(HBaseConstants.Tables.PROFILE_TO_ADD)
    admin.truncateTable(HBaseConstants.Tables.PROFILE_TO_ADD, true)
  }
}
