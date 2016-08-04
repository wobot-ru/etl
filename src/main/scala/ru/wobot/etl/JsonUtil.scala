package ru.wobot.etl

import ru.wobot.etl.dto.DetailedPostDto


object JsonUtil {

  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization

  implicit private val formats: Formats = DefaultFormats

  def toJson(value: AnyRef) = Serialization.write(value)

  def fromJson[T: Manifest](str: String) = parse(str).extract[T](formats, manifest[T])


  def main(args: Array[String]): Unit = {
    val p=new DetailedPostDto(id = "id",
      segment = "segment",
      crawlDate = "crawlDate",
      href = "href",
      source = "href",
      profileId = "profileId",
      smPostId = "smPostId",
      parentPostId = "parentPostId",
      body = "body",
      date = "date",
      engagement = "engagement",
      isComment = true,
      profileCity = null,
      profileGender = "gender",
      profileHref = "href",
      profileName = "name",
      reach = "reach",
      smProfileId = "smProfileId")

    val JSONString=p.toJson()
    println(JSONString)
    println(parse(JSONString).extract[DetailedPostDto](formats, manifest[DetailedPostDto]))
    println()

    val JSONString2 = """
                       {"profileId":"vk://id1","smPostId":"1011991","parentPostId":null,"body":"Разбанили Instagram? \n\nhttps://www.instagram.com/p/BFb_2iDr7Qv/\nhttp://instagramvk.com/p/BFb_2iDr7Qv/","date":"2016-05-16T12:13:18.000+0300","engagement":"61142","isComment":false,"id":"vk://id1/posts/1011991","segment":"20160628181811","crawlDate":"1467127136929","href":"vk://id1/posts/1011991","source":"vk://id1/posts/1011991"}
                      """
    println(parse(JSONString2).extract[DetailedPostDto](formats, manifest[DetailedPostDto]))
  }
}
