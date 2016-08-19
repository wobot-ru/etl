package ru.wobot.etl

import ru.wobot.etl.dto.DetailedPostDto

object JsonUtil {

  import org.json4s.FieldSerializer._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization

  private val detailedPostDtoSerializer = FieldSerializer[DetailedPostDto](
    renameTo("profileCity", "profile_city") orElse
      renameTo("crawlDate", "fetch_time") orElse
      renameTo("profileGender", "profile_gender") orElse
      renameTo("href", "post_href") orElse
      renameTo("profileId", "profile_id") orElse
      renameTo("smPostId", "sm_post_id") orElse
      renameTo("parentPostId", "parent_post_id") orElse
      renameTo("body", "post_body") orElse
      renameTo("date", "post_date") orElse
      renameTo("isComment", "is_comment") orElse
      renameTo("profileHref", "profile_href") orElse
      renameTo("profileName", "profile_name") orElse
      renameTo("smProfileId", "sm_profile_id"),
    renameFrom("profile_city", "profileCity") orElse
      renameFrom("fetch_time", "crawlDate") orElse
      renameFrom("post_href", "href") orElse
      renameFrom("profile_id", "profileId") orElse
      renameFrom("sm_post_id", "smPostId") orElse
      renameFrom("parent_post_id", "parentPostId") orElse
      renameFrom("post_body", "body") orElse
      renameFrom("post_date", "date") orElse
      renameFrom("is_comment", "isComment") orElse
      renameFrom("profile_city", "profileCity") orElse
      renameFrom("profile_gender", "profileGender") orElse
      renameFrom("profile_href", "profileHref") orElse
      renameFrom("profile_name", "profileName") orElse
      renameFrom("sm_profile_id", "smProfileId")
  )

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

    val JSONString3 =
      """
                        {"profile_city":null,"profile_gender":null,"profile_href":"https://www.facebook.com/Tehnosila/","profile_name":"Техносила","reach":"0","sm_profile_id":"102677859783352","profile_id":"fb://102677859783352","sm_post_id":"102677859783352_1109782019072926","parent_post_id":null,"post_body":"Киберпонедельник начинается в пятницу =) Успейте купить товар со скидкой, пока он еще в наличии http://www.tehnosila.ru/action/cyber_monday #честныецены #техносила_меняется #техносиларядом","post_date":"2016-05-27T11:28:06.000+0300","engagement":"13","is_comment":false,"id":"fb://102677859783352/posts/102677859783352_1109782019072926","segment":"20160818145731","fetch_time":"1471521524977","post_href":"https://www.facebook.com/102677859783352/posts/1109782019072926","source":"fb"}
      """
    println(fromJson[DetailedPostDto](JSONString2))
    println(fromJson[DetailedPostDto](JSONString3))
  }
}
