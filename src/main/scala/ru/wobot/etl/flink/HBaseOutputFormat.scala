package ru.wobot.etl.flink

import java.io.IOException

import org.apache.flink.api.common.io.OutputFormat
import org.apache.hadoop.conf.{Configuration => HbaseConf}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import ru.wobot.etl.Profile
import org.apache.flink.configuration.Configuration
import ru.wobot.etl._

class HBaseOutputFormat[T<:Document](tableName: String, formatId:T => String) extends OutputFormat[T] {
  private var conf: HbaseConf = null
  private var table: HTable = null

  private val serialVersionUID: Long = 1L

  override def configure(parameters: Configuration): Unit = conf = HBaseConfiguration.create

  @throws[IOException]
  override def open(taskNumber: Int, numTasks: Int) {
    table = new HTable(conf, tableName)
  }

  @throws[IOException]
  override def writeRecord(d: T) {
    val put: Put = new Put(Bytes.toBytes(formatId(d)))
    put.add(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(d.url))
    put.add(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(d.crawlDate))
    val json = d match {
      case p: Profile => p.profile.toJson()
      case p: Post => p.post.toJson()
    }
    put.add(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(json))
    table.put(put)
  }

  @throws[IOException]
  override def close {
    table.flushCommits
    table.close
  }
}
