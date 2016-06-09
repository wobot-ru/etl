package ru.wobot.etl.flink

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import ru.wobot.etl.dto.{PostDto, ProfileDto}
import ru.wobot.etl.{JsonUtil, Post, Profile}

object InputFormat {
  def profileToProcess() = new ProfileInputFormat(HBaseConstants.T_PROFILE_TO_PROCESS)
  def postToProcess() = new PostInputFormat(HBaseConstants.T_POST_TO_PROCESS)

  def profilesStore() = new ProfileInputFormat(HBaseConstants.T_PROFILE_VIEW)

  class ProfileInputFormat(val tableName: String) extends TableInputFormat[Profile] {
    override def mapResultToTuple(r: Result): Profile = {
      val id = Bytes.toString(r.getValue(HBaseConstants.CF_ID, HBaseConstants.C_ID))
      val crawlDate: Long = Bytes.toLong(r.getValue(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE))
      val json: String = Bytes.toString(r.getValue(HBaseConstants.CF_DATA, HBaseConstants.C_JSON))

      Profile(id, crawlDate, JsonUtil.fromJson[ProfileDto](json))
    }

    override def getTableName = tableName

    override def getScanner: Scan = {
      new Scan()
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_ID)
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE)
        .addColumn(HBaseConstants.CF_DATA, HBaseConstants.C_JSON)
    }
  }
  class PostInputFormat(val tableName: String) extends TableInputFormat[Post] {
    override def mapResultToTuple(r: Result): Post = {
      val id = Bytes.toString(r.getValue(HBaseConstants.CF_ID, HBaseConstants.C_ID))
      val crawlDate: Long = Bytes.toLong(r.getValue(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE))
      val json: String = Bytes.toString(r.getValue(HBaseConstants.CF_DATA, HBaseConstants.C_JSON))

      Post(id, crawlDate, JsonUtil.fromJson[PostDto](json))
    }

    override def getTableName = tableName

    override def getScanner: Scan = {
      new Scan()
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_ID)
        .addColumn(HBaseConstants.CF_ID, HBaseConstants.C_CRAWL_DATE)
        .addColumn(HBaseConstants.CF_DATA, HBaseConstants.C_JSON)
    }
  }

}
