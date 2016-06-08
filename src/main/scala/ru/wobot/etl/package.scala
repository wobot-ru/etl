package ru.wobot

import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.scala.{createTypeInformation => _}
import org.apache.flink.streaming.api.scala._
import ru.wobot.etl.dto.{PostDto, ProfileDto}

package object etl {
  implicit val postTI = createTypeInformation[Post].asInstanceOf[CaseClassTypeInfo[Post]]
  implicit val profileTI = createTypeInformation[Profile].asInstanceOf[CaseClassTypeInfo[Profile]]

  trait Document {
    def url: String

    def crawlDate: Long
  }

  case class Post(url: String, crawlDate: Long, post: PostDto) extends Document

  case class Profile(url: String, crawlDate: Long, profile: ProfileDto) extends Document

  case class ExtractedPaths(profiles: Option[String], posts: Option[String])

  case class ProfileOrPost(url: String, crawlDate: Long, profile: Option[ProfileDto], post: Option[PostDto]) extends Document
}
