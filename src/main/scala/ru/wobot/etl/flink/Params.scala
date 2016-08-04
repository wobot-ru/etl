package ru.wobot.etl.flink

object Params {
  val TOPIC_POST = "topic-post"
  val TOPIC_PROFILE = "topic-profile"

  val DIR_UPLOAD_POST = "dir-upload-post"
  val DETAILED_POST_OUT_DIR = "detailed-post-out-dir"

  val HBASE_EXPORT = "hbase-export"
  val UPLOAD_TO_ES = "upload-to-es"

  val ES_HOSTS = "es-hosts"
  val INDEX_NAME = "index-name"
  val HBASE_OUT_DIR = "hbase-out-dir"
}
