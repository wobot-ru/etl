package ru.wobot.etl.flink.nutch

import ru.wobot.etl.dto.Profile

/**
 * Created by kviz on 6/2/2016.
 */
object Profiles {
  val p1: Profile = new Profile(
    id = "id1",
    segment = "segment",
    crawlDate = "crawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDatecrawlDate",
    href = "href1",
    source = "source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1source1",
    smProfileId = "smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1smProfileId1",
    name = "name 1",
    city = "city1",
    reach = "reach1",
    friendCount = "friend1",
    followerCount = "followers1",
    gender = "man")
  val p2: Profile = new Profile(
    id = "id2",
    segment = "segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2segment2",
    crawlDate = "crawlDate2",
    href = "href2",
    source = "source2",
    smProfileId = "smProfileId2smProfileId2smProfileId2smProfileId2smProfileId2smProfileId2smProfileId2smProfileId2smProfileId2smProfileId2smProfileId2",
    name = "name2",
    city = "city2",
    reach = "reach2",
    friendCount = "friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2friend2",
    followerCount = "followers2",
    gender = "man")
  val p3: Profile = new Profile("id3", "3333333333333333333333333", "1", "333333333333333333333333333333333333333333", "1", "1111111111111111111", "1", "134fdsfsdfsdafsdafsdf", "1", "1", "1", "1")
}
