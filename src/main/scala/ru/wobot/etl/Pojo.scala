package ru.wobot.etl.flink.nutch

import ru.wobot.etl.{Indexable, JsonUtil}

class Pojo(val id2: String, val profileId:String)  {
  override def toString: String = JsonUtil.toJson(this)
}