package ru.wobot.etl.dto

import ru.wobot.etl.JsonUtil

class PostDto(id: String,
              segment: String,
              crawlDate: String,
              href: String,
              source: String,
              val profileId: String,
              val smPostId: String,
              val parentPostId: String,
              val body: String,
              val date: String,
              val engagement: String,
              val isComment: Boolean)
  extends Index(id: String, segment: String, crawlDate: String, href: String, source: String) {

  override def toString: String = toJson()

  def toJson() = JsonUtil.toJson(this)
}

