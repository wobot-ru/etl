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
import ru.wobot.etl._
import ru.wobot.etl.dto.{PostDto, ProfileDto}
import ru.wobot.sm.core.mapping.{PostProperties, ProfileProperties}
import ru.wobot.sm.core.meta.ContentMetaConstants
import ru.wobot.sm.core.parse.ParseResult

class Extractor(val batch: ExecutionEnvironment) {
  batch.getConfig.enableForceKryo()

  def execute(): Unit = {
    try {
      batch.execute("Exporting data from segments...")
    }
    catch {
      case e: RuntimeException =>
        if (!"No new data sinks have been defined since the last execution. The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'."
          .eq(e.getMessage))
          throw e
    }
  }

  def addSegment(segmentPath: Path, postPath:String, profilePath:String): Unit = {
    val exportJob = org.apache.hadoop.mapreduce.Job.getInstance()
    val fs = segmentPath.getFileSystem(exportJob.getConfiguration)
    if (SegmentChecker.isIndexable(segmentPath, fs)) {
      println("Export segment: " + segmentPath)
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentPath, CrawlDatum.FETCH_DIR_NAME))
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentPath, CrawlDatum.PARSE_DIR_NAME))
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentPath, ParseData.DIR_NAME))
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentPath, ParseText.DIR_NAME))

      val input = batch.createInput(new HadoopInputFormat[Text, NutchWritable](new SequenceFileInputFormat[Text, NutchWritable], classOf[Text], classOf[NutchWritable], exportJob))
      val map = input.flatMap((t: (Text, Writable), out: Collector[(Text, NutchWritable)]) => {
        t._2 match {
          case c: CrawlDatum => if (c.getStatus == CrawlDatum.STATUS_FETCH_SUCCESS) out.collect((t._1, new NutchWritable(t._2)))
          case c: ParseData => if (c.getStatus.isSuccess) out.collect((t._1, new NutchWritable(t._2)))
          case c: ParseText => out.collect((t._1, new NutchWritable(t._2)))
        }
      })

      val data = map.groupBy(0).reduceGroup((tuples: Iterator[(Text, NutchWritable)], out: Collector[ProfileOrPost]) => {
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
                val profile = new ProfileDto(key,
                  segment,
                  crawlDate,
                  parseMeta.get(ProfileProperties.HREF),
                  parseMeta.get(ProfileProperties.SOURCE),
                  parseMeta.get(ProfileProperties.SM_PROFILE_ID),
                  parseMeta.get(ProfileProperties.NAME),
                  parseMeta.get(ProfileProperties.CITY),
                  parseMeta.get(ProfileProperties.REACH),
                  parseMeta.get(ProfileProperties.FRIEND_COUNT),
                  parseMeta.get(ProfileProperties.FOLLOWER_COUNT),
                  parseMeta.get(ProfileProperties.GENDER)
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
                      href = parseResult.getUrl,
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
  }
}
