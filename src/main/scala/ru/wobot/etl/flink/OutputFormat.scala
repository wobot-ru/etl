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
import ru.wobot.etl.dto.DetailedPostDto

object OutputFormat {
  def postsToES() = new HBaseOutputFormat[DetailedPostDto](HBaseConstants.T_POST_TO_ES, p => {
    new Put(Bytes.toBytes(s"${p.id}"))
      .add(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.id))
      .add(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
      .add(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.toJson))
  })


  def profilesToProces() = new HBaseOutputFormat[Profile](HBaseConstants.T_PROFILE_TO_PROCESS, p => {
    new Put(Bytes.toBytes(s"${p.url}|${p.crawlDate}"))
      .add(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.url))
      .add(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
      .add(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.profile.toJson))
  })

  def profilesStore() = new HBaseOutputFormat[Profile](HBaseConstants.T_PROFILE_VIEW, p => {
    new Put(Bytes.toBytes(s"${p.url}"))
      .add(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.url))
      .add(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
      .add(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.profile.toJson))
  })

  def postsToProces() = new HBaseOutputFormat[Post](HBaseConstants.T_POST_TO_PROCESS,
    p => {
      new Put(Bytes.toBytes(s"${p.url}|${p.crawlDate}"))
        .add(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.url))
        .add(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
        .add(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.post.toJson))
    })

  def postsWithoutProfile() = new HBaseOutputFormat[Post](HBaseConstants.T_POST_WITHOUT_PROFILE,
    p => {
      new Put(Bytes.toBytes(s"${p.url}|${p.crawlDate}"))
        .add(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.url))
        .add(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
        .add(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.post.toJson))
    })

  class HBaseOutputFormat[T](tableName: String, write: T => Put) extends OutputFormat[T] {
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
      table.put(write(d))
    }

    @throws[IOException]
    override def close {
      table.flushCommits
      table.close
    }
  }

}
