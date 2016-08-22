package ru.wobot

import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.scala.{createTypeInformation => _}
import org.apache.flink.streaming.api.scala._
import ru.wobot.etl.dto.{DetailedPostDto, PostDto, ProfileDto}

package object etl {
  implicit val postTI = createTypeInformation[Post].asInstanceOf[CaseClassTypeInfo[Post]]
  implicit val profileTI = createTypeInformation[Profile].asInstanceOf[CaseClassTypeInfo[Profile]]
  implicit val detailedPostTI = createTypeInformation[DetailedPost].asInstanceOf[CaseClassTypeInfo[DetailedPost]]
  implicit val pageTI = createTypeInformation[Page].asInstanceOf[CaseClassTypeInfo[Page]]
  implicit val ajacencyTI = createTypeInformation[Adjacency].asInstanceOf[CaseClassTypeInfo[Adjacency]]

  trait Document {
    def url: String

    def crawlDate: Long
  }

  case class Post(url: String, crawlDate: Long, post: PostDto) extends Document

  case class DetailedPost(url: String, crawlDate: Long, post: DetailedPostDto) extends Document

  case class Profile(url: String, crawlDate: Long, profile: ProfileDto) extends Document

  case class ExtractedPaths(profiles: Option[String], posts: Option[String])

  case class ProfileOrPost(url: String, crawlDate: Long, profile: Option[ProfileDto], post: Option[PostDto]) extends Document

  case class DetailedOrWithoutAuthorPost(detailed:Option[DetailedPostDto], withoutAuthor:Option[Post])

  case class Page(url: String, crawlDate: Long, rank: Double) extends Document

  case class Adjacency(url: String, crawlDate: Long, neighbors: Array[String]) extends Document {
    override def toString: String = s"Adjacency($url,$crawlDate,[${neighbors.mkString(",")}])"
  }
}
