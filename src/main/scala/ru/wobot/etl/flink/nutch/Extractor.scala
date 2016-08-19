package ru.wobot.etl.flink.nutch

import com.google.gson.Gson
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.io.TypeSerializerOutputFormat
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.nutch.crawl.{CrawlDatum, NutchWritable}
import org.apache.nutch.metadata.Metadata
import org.apache.nutch.parse.{ParseData, ParseText}
import org.apache.nutch.segment.SegmentChecker
import org.apache.nutch.util.StringUtil
import org.slf4j.LoggerFactory
import ru.wobot.etl._
import ru.wobot.etl.dto.{PostDto, ProfileDto}
import ru.wobot.sm.core.mapping.{PostProperties, ProfileProperties}
import ru.wobot.sm.core.meta.ContentMetaConstants
import ru.wobot.sm.core.parse.ParseResult

class Extractor(val batch: ExecutionEnvironment) {
  private val LOGGER = LoggerFactory.getLogger(classOf[Extractor])
  batch.getConfig.enableForceKryo()

  def execute(segmentPath: String): Unit = {
    try {
      batch.execute(s"Export data from segment $segmentPath")
    }
    catch {
      case e: RuntimeException =>
        if (!"No new data sinks have been defined since the last execution. The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'."
          .eq(e.getMessage))
          throw e
    }
  }

  def addSegment(segmentPath: Path, postPath: String, profilePath: String): Unit = {
    val exportJob = org.apache.hadoop.mapreduce.Job.getInstance()
    val fs = segmentPath.getFileSystem(exportJob.getConfiguration)
    if (SegmentChecker.isIndexable(segmentPath, fs)) {
      val fetchDir: Path = new Path(segmentPath, CrawlDatum.FETCH_DIR_NAME)
      val parseDir: Path = new Path(segmentPath, CrawlDatum.PARSE_DIR_NAME)
      val parseData: Path = new Path(segmentPath, ParseData.DIR_NAME)
      val parseText: Path = new Path(segmentPath, ParseText.DIR_NAME)

      if (fs.exists(fetchDir) && fs.exists(parseDir) && fs.exists(parseData) && fs.exists(parseText)) {
        LOGGER.info(s"Extract segment: $segmentPath")
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, fetchDir)
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, parseDir)
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, parseData)
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, parseText)

        val input = batch.createInput(new HadoopInputFormat[Text, NutchWritable](new SequenceFileInputFormat[Text, NutchWritable], classOf[Text], classOf[NutchWritable], exportJob))
        val map = input.flatMap((t: (Text, Writable), out: Collector[(Text, NutchWritable)]) => {
          t._2 match {
            case c: CrawlDatum => if (c.getStatus == CrawlDatum.STATUS_FETCH_SUCCESS) out.collect((t._1, new NutchWritable(t._2)))
            case c: ParseData => if (c.getStatus.isSuccess) out.collect((t._1, new NutchWritable(t._2)))
            case c: ParseText => out.collect((t._1, new NutchWritable(t._2)))
          }
        })

        val data = map.rebalance().groupBy(0).reduceGroup((tuples: Iterator[(Text, NutchWritable)], out: Collector[ProfileOrPost]) => {
          val gson = new Gson()
          def fromJson[T](json: String, clazz: Class[T]): T = {
            return gson.fromJson(json, clazz)
          }

          var key: String = null
          var fetchDatum: CrawlDatum = null
          var parseData: ParseData = null
          var parseText: ParseText = null

          for ((url, data) <- tuples) {
            data.get() match {
              case c: CrawlDatum => {
                key = url.toString
                fetchDatum = c
              }
              case d: ParseData => parseData = d
              case t: ParseText => parseText = t
              case _ => ()
            }
          }
          if (parseData != null && fetchDatum != null) {
            val contentMeta = parseData.getContentMeta
            val skipFromElastic: String = contentMeta.get(ContentMetaConstants.SKIP_FROM_ELASTIC_INDEX)
            if (skipFromElastic == null || !skipFromElastic.equals("1")) {
              val fetchTime: Long = fetchDatum.getFetchTime
              val crawlDate: String = fetchTime.toString
              val segment = contentMeta.get(org.apache.nutch.metadata.Nutch.SEGMENT_NAME_KEY);
              val isSingleDoc: Boolean = !"true".equals(contentMeta.get(ContentMetaConstants.MULTIPLE_PARSE_RESULT))
              if (isSingleDoc) {
                val parseMeta: Metadata = parseData.getParseMeta
                val subType = contentMeta.get(ContentMetaConstants.TYPE);
                if (subType != null && subType.equals(ru.wobot.sm.core.mapping.Types.PROFILE)) {
                  val profile = new ProfileDto(id = key,
                    segment = segment,
                    crawlDate = crawlDate,
                    href = parseMeta.get(ProfileProperties.HREF),
                    source = parseMeta.get(ProfileProperties.SOURCE),
                    smProfileId = parseMeta.get(ProfileProperties.SM_PROFILE_ID),
                    name = parseMeta.get(ProfileProperties.NAME),
                    city = parseMeta.get(ProfileProperties.CITY),
                    reach = parseMeta.get(ProfileProperties.REACH),
                    friendCount = parseMeta.get(ProfileProperties.FRIEND_COUNT),
                    followerCount = parseMeta.get(ProfileProperties.FOLLOWER_COUNT),
                    gender = parseMeta.get(ProfileProperties.GENDER)
                  )
                  out.collect(ProfileOrPost(profile.id, fetchTime, Some(profile), None))
                }
              }
              else if (parseText != null) {
                val content: String = parseText.getText()
                if (!StringUtil.isEmpty(content)) {
                  val parseResults: Array[ParseResult] = fromJson[Array[ParseResult]](parseText.getText, classOf[Array[ParseResult]])
                  if (parseResults != null) for (parseResult <- parseResults) {
                    var subType: String = parseResult.getContentMeta.get(ContentMetaConstants.TYPE).asInstanceOf[String]
                    if (subType == null) subType = parseData.getContentMeta.get(ContentMetaConstants.TYPE)
                    if (subType == ru.wobot.sm.core.mapping.Types.POST) {
                      val parseMeta = parseResult.getParseMeta
                      val post = new PostDto(id = parseResult.getUrl,
                        segment = segment,
                        crawlDate = crawlDate,
                        href = parseMeta.get(PostProperties.HREF).asInstanceOf[String],
                        source = parseMeta.get(ProfileProperties.SOURCE).asInstanceOf[String],
                        profileId = parseMeta.get(PostProperties.PROFILE_ID).asInstanceOf[String],
                        smPostId = String.valueOf(parseMeta.get(PostProperties.SM_POST_ID)).replace(".0", ""),
                        parentPostId = parseMeta.get(PostProperties.PARENT_POST_ID).asInstanceOf[String],
                        body = parseMeta.get(PostProperties.BODY).asInstanceOf[String],
                        date = parseMeta.get(PostProperties.POST_DATE).asInstanceOf[String],
                        engagement = String.valueOf(parseMeta.get(PostProperties.ENGAGEMENT)).replace(".0", ""),
                        isComment = parseMeta.get(PostProperties.IS_COMMENT).asInstanceOf[Boolean]
                      )

                      out.collect(ProfileOrPost(post.id, fetchTime, None, Some(post)))
                    } else if (subType == ru.wobot.sm.core.mapping.Types.PROFILE) {
                      val parseMeta = parseResult.getParseMeta
                      val profile = new ProfileDto(id = parseResult.getUrl,
                        segment = segment,
                        crawlDate = crawlDate,
                        href = parseMeta.get(ProfileProperties.HREF).asInstanceOf[String],
                        source = parseMeta.get(ProfileProperties.SOURCE).asInstanceOf[String],
                        smProfileId = parseMeta.get(ProfileProperties.SM_PROFILE_ID).asInstanceOf[String],
                        name = parseMeta.get(ProfileProperties.NAME).asInstanceOf[String],
                        city = parseMeta.get(ProfileProperties.CITY).asInstanceOf[String],
                        reach = String.valueOf(parseMeta.get(ProfileProperties.REACH)).replace(".0", ""),
                        friendCount = String.valueOf(parseMeta.get(ProfileProperties.FRIEND_COUNT)).replace(".0", ""),
                        followerCount = String.valueOf(parseMeta.get(ProfileProperties.FOLLOWER_COUNT)).replace(".0", ""),
                        gender = parseMeta.get(ProfileProperties.GENDER).asInstanceOf[String]
                      )
                      out.collect(ProfileOrPost(profile.id, fetchTime, Some(profile), None))
                    }
                  }
                }
              }
            }
          }
        })


        val unic = data.sortPartition(0, Order.ASCENDING)
        val posts = unic.filter(x => x.post.isDefined).map(r => Post(r.url, r.crawlDate, r.post.get))
        val profiles = unic.filter(x => x.profile.isDefined).map(r => Profile(r.url, r.crawlDate, r.profile.get))

        posts.write(new TypeSerializerOutputFormat[Post], postPath, WriteMode.OVERWRITE)
        profiles.write(new TypeSerializerOutputFormat[Profile], profilePath, WriteMode.OVERWRITE)
      }
      else
        LOGGER.info(s"Skip segment: $segmentPath")
    }
  }
}
