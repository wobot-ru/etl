package ru.wobot

import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.scala.{createTypeInformation => _}
import org.apache.flink.streaming.api.scala._
import ru.wobot.etl.dto.{Post, Profile}

package object etl {
  implicit val postTI = createTypeInformation[PostRow].asInstanceOf[CaseClassTypeInfo[PostRow]]
  implicit val profileTI = createTypeInformation[ProfileRow].asInstanceOf[CaseClassTypeInfo[ProfileRow]]

  case class PostRow(url: String, crawlDate: Long, post: Post)

  case class ProfileRow(url: String, crawlDate: Long, profile: Profile)

  case class PostOrRow(url: String, crawlDate: Long, post:Option[Post], profile:Option[Profile])
}
