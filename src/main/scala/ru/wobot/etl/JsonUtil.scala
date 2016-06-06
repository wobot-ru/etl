package ru.wobot.etl

import ru.wobot.etl.dto.{PostDto, ProfileDto}


object JsonUtil {

  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization

  implicit private val formats = DefaultFormats + FieldSerializer[PostDto]() + FieldSerializer[ProfileDto]()

  def toJson(value: AnyRef) = Serialization.write(value)

  def fromJson[T: Manifest](str: String) = parse(str).extract[T](formats, manifest[T])
}
