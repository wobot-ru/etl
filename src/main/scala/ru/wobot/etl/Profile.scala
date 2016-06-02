package ru.wobot.etl


class Profile(val id: String,
              val segment: String,
              val crawlDate: String,
              val href: String,
              val source: String,
              val smProfileId: String,
              val name: String,
              val city: String,
              val reach: String,
              val friendCount: String,
              val followerCount: String,
              val gender: String) {

  override def toString: String = JsonUtil.toJson(this)
}
