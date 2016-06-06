package ru.wobot.etl.flink.nutch

import java.util.Properties

import com.google.gson.Gson
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.nutch.crawl.{CrawlDatum, NutchWritable}
import org.apache.nutch.metadata.Metadata
import org.apache.nutch.parse.{ParseData, ParseText}
import org.apache.nutch.segment.SegmentChecker
import org.apache.nutch.util.{HadoopFSUtil, StringUtil}
import ru.wobot.etl._
import ru.wobot.etl.dto.{Post, Profile}
import ru.wobot.sm.core.mapping.{PostProperties, ProfileProperties}
import ru.wobot.sm.core.meta.ContentMetaConstants
import ru.wobot.sm.core.parse.ParseResult

object SegmentExport {
  val batch = ExecutionEnvironment.getExecutionEnvironment
  val stream = StreamExecutionEnvironment.getExecutionEnvironment


  var properties: Properties = null

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val params = ParameterTool.fromArgs(args)
    properties = params.getProperties
    properties.setProperty("bootstrap.servers", "localhost:9092")

    stream.getConfig.disableSysoutLogging
    //    stream.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    //    stream.enableCheckpointing(5000)

    batch.getConfig.enableForceKryo()
    stream.getConfig.enableForceKryo()

    if (params.has("seg"))
      addSegment(new Path(params.getRequired("seg")))

    if (params.has("dir")) {
      val segmentIn = new Path(params.getRequired("dir"))
      val fs = segmentIn.getFileSystem(new JobConf())
      val segments = HadoopFSUtil.getPaths(fs.listStatus(segmentIn, HadoopFSUtil.getPassDirectoriesFilter(fs)))
      for (dir <- segments) {
        addSegment(dir)

        val profilePath = new Path(dir, "parse-profiles").toString
        stream
          .readFile(new TypeSerializerInputFormat[ProfileRow](profileTI), profilePath)
          .addSink(new FlinkKafkaProducer09[ProfileRow]("profiles", new TypeInformationSerializationSchema[ProfileRow](profileTI, stream.getConfig), properties))

        val postPath = new Path(dir, "parse-posts").toString
        stream
          .readFile(new TypeSerializerInputFormat[PostRow](postTI), postPath)
          .addSink(new FlinkKafkaProducer09[PostRow]("posts", new TypeInformationSerializationSchema[PostRow](postTI, stream.getConfig), properties))
      }
    }
    try {
      batch.execute("Exporting data from segments...")
      stream.execute("Publish to kafka...")
    }
    catch {
      case e: RuntimeException =>
        if (!"No new data sinks have been defined since the last execution. The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'."
          .eq(e.getMessage))
          throw e
    }
    finally {
      val elapsedTime = System.currentTimeMillis() - startTime
      println("Export finish, elapsedTime=" + elapsedTime)
    }
  }


  def addSegment(segmentPath: Path): Unit = {
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

      val data = map.groupBy(0).reduceGroup((tuples: Iterator[(Text, NutchWritable)], out: Collector[PostOrRow]) => {
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
                val profile = new Profile(key,
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
                out.collect(PostOrRow(profile.id, fetchTime, None, Some(profile)))
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
                    val post = new Post(id = parseResult.getUrl,
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

                    out.collect(PostOrRow(post.id, fetchTime, Some(post), None))
                  }
                }
              }
            }
          }
        }
      })

      val postPath = new Path(segmentPath, "parse-posts").toString
      val profilePath = new Path(segmentPath, "parse-profiles").toString

      val unic = data.sortPartition(0, Order.ASCENDING)
      val posts = unic.filter(x => x.post.isDefined).map(r => PostRow(r.url, r.crawlDate, r.post.get))
      val profiles = unic.filter(x => x.profile.isDefined).map(r => ProfileRow(r.url, r.crawlDate, r.profile.get))

      posts.write(new TypeSerializerOutputFormat[PostRow], postPath, WriteMode.OVERWRITE)
      profiles.write(new TypeSerializerOutputFormat[ProfileRow], profilePath, WriteMode.OVERWRITE)
    }
  }
}
