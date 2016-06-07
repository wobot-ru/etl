package ru.wobot.etl.dto

import ru.wobot.etl.JsonUtil

class ProfileDto(id: String,
                 segment: String,
                 crawlDate: String,
                 href: String,
                 source: String,
                 val smProfileId: String,
                 val name: String,
                 val city: String,
                 val reach: String,
                 val friendCount: String,
                 val followerCount: String,
                 val gender: String)
  extends Index(id: String, segment: String, crawlDate: String, href: String, source: String) {

  override def toString: String = JsonUtil.toJson(this)

  def toJson() = JsonUtil.toJson(this)
}