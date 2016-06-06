package ru.wobot.etl.dto

import ru.wobot.etl.JsonUtil

class Post(val id: String,
           val segment: String,
           val crawlDate: String,
           val href: String,
           val source: String,
           val profileId: String,
           val smPostId: String,
           val parentPostId: String,
           val body: String,
           val date: String,
           val engagement: String,
           val isComment: Boolean
          )
//extends Indexable(id: String, segment: String, crawlDate: String, href: String, source: String)
{
  override def toString: String = JsonUtil.toJson(this)
}

