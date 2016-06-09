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

class DetailedPostDto(id: String,
                      segment: String,
                      crawlDate: String,
                      href: String,
                      source: String,
                      profileId: String,
                      smPostId: String,
                      parentPostId: String,
                      body: String,
                      date: String,
                      engagement: String,
                      isComment: Boolean,
                      profileCity: String,
                      profileGender: String,
                      profileHref: String,
                      profileName: String,
                      reach: String,
                      smProfileId: String)
  extends PostDto(id, segment, crawlDate, href, source, profileId, smPostId, parentPostId, body, date, engagement, isComment) {

  override def toString: String = toJson()

  override def toJson() = JsonUtil.toJson(this)
}