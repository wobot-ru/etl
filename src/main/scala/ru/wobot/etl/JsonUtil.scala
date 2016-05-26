package ru.wobot.etl


object JsonUtil {
  import org.json4s._
  import org.json4s.jackson.Serialization

  implicit val formats = DefaultFormats + FieldSerializer[Post]()+ FieldSerializer[Profile]()

  def toJson(value: Any): String = {
    val value1: AnyRef = value.asInstanceOf[AnyRef]
    Serialization.write(value1)
  }
}
