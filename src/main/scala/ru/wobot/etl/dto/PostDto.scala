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

  def toJson(): String = JsonUtil.toJson(this)

  def copy(id: String = this.asInstanceOf[Index].id,
           segment: String = this.asInstanceOf[Index].segment,
           crawlDate: String = this.asInstanceOf[Index].crawlDate,
           href: String = this.asInstanceOf[Index].href,
           source: String = this.asInstanceOf[Index].source,
           profileId: String = profileId,
           smPostId: String = smPostId,
           parentPostId: String = parentPostId,
           body: String = body,
           date: String = date,
           engagement: String = engagement,
           isComment: Boolean = isComment): PostDto =
    new PostDto(id = id,
      segment = segment,
      crawlDate = crawlDate,
      href = href,
      source = source,
      profileId = profileId,
      smPostId = smPostId,
      parentPostId = parentPostId,
      body = body,
      date = date,
      engagement = engagement,
      isComment = isComment)
}