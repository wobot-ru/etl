package ru.wobot.etl


class Profile extends Indexable {
  var smProfileId: String = _
  var name: String = _
  var city: String = _
  var reach: String = _
  var friendCount: String = _
  var followerCount: String = _
  var gender: String = _

  override def toString: String = JsonUtil.toJson(this)
}
