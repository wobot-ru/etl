package ru.wobot.etl.flink

import org.apache.hadoop.hbase.util.Bytes

object HBaseConstants {
  val CF_ID = Bytes.toBytes("id")
  val CF_DATA = Bytes.toBytes("data")
  val C_ID = Bytes.toBytes("id")
  val C_CRAWL_DATE = Bytes.toBytes("crawlDate")
  val C_JSON = Bytes.toBytes("json")

  val T_PROFILE = "profile"
  val T_PROFILE_TO_ADD = "profile-to-add"
}
