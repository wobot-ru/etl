package ru.wobot.etl.flink

import java.io.IOException

import org.apache.flink.api.common.io.OutputFormat
import org.apache.hadoop.conf.{Configuration => HbaseConf}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import ru.wobot.etl.Profile
import org.apache.flink.configuration.Configuration

class HBaseOutputFormat(tableName: String, formatId: Profile => String) extends OutputFormat[Profile] {
  private var conf: HbaseConf = null
  private var table: HTable = null

  private val serialVersionUID: Long = 1L

  override def configure(parameters: Configuration): Unit = conf = HBaseConfiguration.create

  @throws[IOException]
  override def open(taskNumber: Int, numTasks: Int) {
    table = new HTable(conf, tableName)
  }

  @throws[IOException]
  override def writeRecord(p: Profile) {
    val put: Put = new Put(Bytes.toBytes(formatId(p)))
    put.add(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.url))
    put.add(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
    put.add(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.profile.toJson()))
    table.put(put)
  }

  @throws[IOException]
  override def close {
    table.flushCommits
    table.close
  }
}
