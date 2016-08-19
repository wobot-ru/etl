package ru.wobot.etl.dto

import ru.wobot.etl.JsonUtil

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
                      val profileCity: String,
                      val profileGender: String,
                      val profileHref: String,
                      val profileName: String,
                      val reach: String,
                      val smProfileId: String)
  extends PostDto(id, segment, crawlDate, href, source, profileId, smPostId, parentPostId, body, date, engagement, isComment) {

  override def toString: String = toJson

  override def toJson: String = JsonUtil.toJson(this)

}
