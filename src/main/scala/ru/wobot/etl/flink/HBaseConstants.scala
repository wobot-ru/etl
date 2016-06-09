package ru.wobot.etl.flink

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes

object HBaseConstants {
  val CF_ID = Bytes.toBytes("id")
  val CF_DATA = Bytes.toBytes("data")
  val C_ID = Bytes.toBytes("id")
  val C_CRAWL_DATE = Bytes.toBytes("crawlDate")
  val C_JSON = Bytes.toBytes("json")

  val T_PROFILE_VIEW = "profile"
  val T_PROFILE_TO_PROCESS = "profile-to-process"
  val T_POST_TO_PROCESS = "post-to-process"
  val T_POST_TO_ES = "post-to-es"
  val T_POST_WITHOUT_PROFILE = "post-without-profile"

  object Tables{
    val PROFILE_TO_PROCESS = TableName.valueOf(HBaseConstants.T_PROFILE_TO_PROCESS)
  }
}
