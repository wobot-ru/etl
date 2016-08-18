package ru.wobot.etl

import ru.wobot.etl.dto.DetailedPostDto

object JsonUtil {

  import org.json4s.FieldSerializer._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization

  private val detailedPostDtoSerializer = FieldSerializer[DetailedPostDto] {
    renameTo("profileCity", "profile_city")
    renameTo("crawlDate", "fetch_time")
    renameTo("profileGender", "profile_gender")
  }

  implicit private val formats: Formats = DefaultFormats + detailedPostDtoSerializer

  def toJson(value: AnyRef) = Serialization.write(value)

  def fromJson[T: Manifest](str: String) = parse(str).extract[T](formats, manifest[T])

  def main(args: Array[String]) = {
    val p = new DetailedPostDto(id = "id",
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

    val JSONString = p.toJson
    println(JSONString)
    println()

    val JSONString2 =
      """
                        {"profileCity":"Moscow","profileGender":null,"profileHref":"https://www.facebook.com/Softline.events/","profileName":"IT-мероприятия","reach":"2630","smProfileId":"146214462119256","profileId":"fb://146214462119256","smPostId":"146214462119256_1027746140632746","parentPostId":null,"body":"Коллеги, приглашаем на конференцию Google Cloud Day, которая состоится 21 апреля в Москве!\nПодробности и регистрация по ссылке: http://events.softline.ru/event/google_cloud_day_383 Первый в этом году семинар, посвященный облачным сервисам Google Apps for Work состоится в Москве 21 апреля 2016!\n\nGoogle Cloud Day - конференция для руководителей эффективного бизнеса.\nЭффективность бизнеса - цель любого руководителя. Что под ней понимать решает каждый сам для себя. Но одно понятно наверняка - слаженная работа бизнес-команд это ключ к успеху.\nЖдем вас!","date":"2016-04-11T15:19:22.000+0300","engagement":"0","isComment":false,"id":"fb://146214462119256/posts/146214462119256_1027746140632746","segment":"20160817143312","crawlDate":"1471434127594","href":"https://www.facebook.com/146214462119256/posts/1027746140632746","source":"fb"}
      """
    //println(parse(JSONString2).extract[DetailedPostDto](formats, manifest[DetailedPostDto]))
    println(fromJson[DetailedPostDto](JSONString2))
  }
}
