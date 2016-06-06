package ru.wobot

import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.scala.{createTypeInformation => _}
import org.apache.flink.streaming.api.scala._
import ru.wobot.etl.dto.{PostDto, ProfileDto}

package object etl {
  implicit val postTI = createTypeInformation[Post].asInstanceOf[CaseClassTypeInfo[Post]]
  implicit val profileTI = createTypeInformation[Profile].asInstanceOf[CaseClassTypeInfo[Profile]]

  case class Post(url: String, crawlDate: Long, post: PostDto)

  case class Profile(url: String, crawlDate: Long, profile: ProfileDto)

  case class ProfileOrPost(url: String, crawlDate: Long, profile: Option[ProfileDto], post: Option[PostDto])

  case class ExtractedPaths(profiles:Option[String], posts:Option[String])
}
