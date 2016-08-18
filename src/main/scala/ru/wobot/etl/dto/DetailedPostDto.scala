package ru.wobot.etl.dto

import ru.wobot.etl.JsonUtil
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

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

  override def toJson: String = {
    /*val json: JsonAST.JObject = ("id" -> id) ~
      ("segment" -> segment) ~
      ("post_href" -> href)*/
    /*val json = Map(
      "id" -> id,
      "segment" -> segment,
      "fetch_time" -> crawlDate,
      "post_href" -> href,
      "source" -> source,
      "profile_id" -> profileId,
      "sm_post_id" -> smPostId,
      "parent_post_id" -> parentPostId,
      "post_body" -> body,
      "post_date" -> date,
      "engagement" -> engagement,
      "is_comment" -> isComment,
      "profile_city" -> profileCity,
      "profile_gender" -> profileGender,
      "profile_href" -> profileHref,
      "profile_name" -> profileName,
      "reach" -> reach,
      "sm_profile_id" -> smProfileId
    )*/
    JsonUtil.toJson(this)
  }
}
