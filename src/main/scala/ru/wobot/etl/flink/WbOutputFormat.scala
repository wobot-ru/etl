package ru.wobot.etl.flink

import java.io.IOException

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.conf.{Configuration => HbaseConf}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import ru.wobot.etl.dto.DetailedPostDto
import ru.wobot.etl.{Profile, _}

object WbOutputFormat {
  def postsToES() = new HBaseOutputFormat[DetailedPostDto](HBaseConstants.T_POST_TO_ES, p => {
    new Put(Bytes.toBytes(s"${p.id}"))
      .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.id))
      .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
      .addColumn(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.toJson))
  })

  def postsStore() = new HBaseOutputFormat[Post](HBaseConstants.T_POST_VIEW, p => {
    new Put(Bytes.toBytes(s"${p.url}"))
      .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.url))
      .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
      .addColumn(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.post.toJson))
  })


  def profilesToProcess() = new HBaseOutputFormat[Profile](HBaseConstants.T_PROFILE_TO_PROCESS, p => {
    new Put(Bytes.toBytes(s"${p.url}|${p.crawlDate}"))
      .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.url))
      .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
      .addColumn(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.profile.toJson()))
  })

  def profilesStore() = new HBaseOutputFormat[Profile](HBaseConstants.T_PROFILE_VIEW, p => {
    new Put(Bytes.toBytes(s"${p.url}"))
      .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.url))
      .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
      .addColumn(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.profile.toJson()))
  })


  def postsToProcess() = new HBaseOutputFormat[Post](HBaseConstants.T_POST_TO_PROCESS,
    p => {
      new Put(Bytes.toBytes(s"${p.url}|${p.crawlDate}"))
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.url))
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
        .addColumn(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.post.toJson))
    })

  def postsWithoutProfile() = new HBaseOutputFormat[Post](HBaseConstants.T_POST_WITHOUT_PROFILE,
    p => {
      new Put(Bytes.toBytes(s"${p.url}|${p.crawlDate}"))
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_ID, Bytes.toBytes(p.url))
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
        .addColumn(HBaseConstants.CF_DATA, HBaseConstants.C_JSON, Bytes.toBytes(p.post.toJson))
    })

  class HBaseOutputFormat[T](tableName: String, write: T => Put) extends OutputFormat[T] {
    private var conf: HbaseConf = null
    private var connection: Connection = null

    private val serialVersionUID: Long = 1L

    override def configure(parameters: Configuration): Unit = conf = HBaseConfiguration.create

    @throws[IOException]
    override def open(taskNumber: Int, numTasks: Int) {
      connection = ConnectionFactory.createConnection(conf)
    }

    @throws[IOException]
    override def writeRecord(d: T) {
      connection.getTable(TableName.valueOf(tableName)).put(write(d))
    }

    @throws[IOException]
    override def close() {
      if (connection != null)
        connection.close()
    }
  }

}

