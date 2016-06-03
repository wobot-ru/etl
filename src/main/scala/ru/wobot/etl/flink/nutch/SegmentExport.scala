package ru.wobot.etl.flink.nutch

import java.util.Properties

import org.apache.flink.api.java.tuple
import com.google.gson.Gson
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.core
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.nutch.crawl.{CrawlDatum, NutchWritable}
import org.apache.nutch.metadata.{Metadata, Nutch}
import org.apache.nutch.parse.{ParseData, ParseText}
import org.apache.nutch.segment.SegmentChecker
import org.apache.nutch.util.{HadoopFSUtil, StringUtil}
import ru.wobot.etl.{Post, Profile}
import ru.wobot.sm.core.mapping.{PostProperties, ProfileProperties}
import ru.wobot.sm.core.meta.ContentMetaConstants
import ru.wobot.sm.core.parse.ParseResult

//import org.apache.flink.api.scala.typeutils._

object SegmentExport {
  val batch = ExecutionEnvironment.getExecutionEnvironment
  val stream = StreamExecutionEnvironment.getExecutionEnvironment
  implicit val postTI = createTypeInformation[(String, Long, Post)].asInstanceOf[CaseClassTypeInfo[(String, Long, Post)]]
  implicit val profileTI = createTypeInformation[(String, Long, Profile)].asInstanceOf[CaseClassTypeInfo[(String, Long, Profile)]]

  val postOutFormat: TypeSerializerOutputFormat[(String, Long, Post)] = new TypeSerializerOutputFormat[(String, Long, Post)]
  val postInFormat = new TypeSerializerInputFormat[(String, Long, Post)](postTI)
  val postSchema = new TypeInformationSerializationSchema[(String, Long, Post)](postTI, stream.getConfig)

  val profileOutFormat = new TypeSerializerOutputFormat[(String, Long, Profile)]
  val profileInFormat = new TypeSerializerInputFormat[(String, Long, Profile)](profileTI)
  val profileSchema = new TypeInformationSerializationSchema[(String, Long, Profile)](profileTI, stream.getConfig)

  var properties: Properties = null

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val params = ParameterTool.fromArgs(args)
    properties = params.getProperties
    properties.setProperty("bootstrap.servers", "localhost:9092")

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

      val data = map.groupBy(0).reduceGroup((tuples: Iterator[(Text, NutchWritable)], out: Collector[(String, Long, Option[Post], Option[Profile])]) => {
        val gson = new Gson()
        def fromJson[T](json: String, clazz: Class[T]): T = {
          return gson.fromJson(json, clazz)
        }

        var key: String = null
        var fetchDatum: CrawlDatum = null
        var parseData: ParseData = null
        var parseText: ParseText = null

        val toArray: Array[(Text, NutchWritable)] = tuples.toArray
        for ((url, data) <- toArray) {
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
            val segment = contentMeta.get(Nutch.SEGMENT_NAME_KEY);
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
                out.collect(profile.id, fetchTime, None, Some(profile))
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

                    //if (crawlDate != null)
                    out.collect(post.id, fetchTime, Some(post), None)
                  }
                }
              }
            }
          }
        }
      })

      val postPath = new Path(segmentPath, "parse-posts").toString
      val profilePath = new Path(segmentPath, "parse-profiles").toString

      val unic: DataSet[(String, Long, Option[Post], Option[Profile])] = data.sortPartition(0, Order.ASCENDING)
      val posts = unic.filter(x => x._3.isDefined).map((tuple: (String, Long, Option[Post], Option[Profile])) => (tuple._1, tuple._2, tuple._3.get))
      val profiles = unic.filter(x => x._4.isDefined).map((tuple: (String, Long, Option[Post], Option[Profile])) => (tuple._1, tuple._2, tuple._4.get))

      posts.write(postOutFormat, postPath, WriteMode.OVERWRITE)
      profiles.write(profileOutFormat, profilePath, WriteMode.OVERWRITE)

      //      val out = new TypeSerializerOutputFormat[(String, Long, Profile)]
      //      out.setOutputFilePath(new core.fs.Path(s"file:////c:\\tmp\\flink\\${segmentPath.getName}"))
      //      out.setWriteMode(WriteMode.OVERWRITE)
      val profileJTI = new TupleTypeInfo[tuple.Tuple3[String, Long, Profile]](
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.createTypeInfo(classOf[Profile]))
      val postJTI = new TupleTypeInfo[tuple.Tuple3[String, Long, Post]](
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.createTypeInfo(classOf[Post]))

      stream
        .readFile(new TypeSerializerInputFormat[tuple.Tuple3[String, Long, Profile]](profileJTI), profilePath)
        .addSink(new FlinkKafkaProducer09[tuple.Tuple3[String, Long, Profile]]("profiles", new TypeInformationSerializationSchema[Tuple3[String, Long, Profile]](profileJTI, stream.getConfig), properties))

      stream
        .readFile(new TypeSerializerInputFormat[tuple.Tuple3[String, Long, Post]](postJTI), postPath)
        .addSink(new FlinkKafkaProducer09[tuple.Tuple3[String, Long, Post]]("posts", new TypeInformationSerializationSchema[Tuple3[String, Long, Post]](postJTI, stream.getConfig), properties))
      //      stream
      //        .readFile(postInFormat, postPath)
      //        .addSink(new FlinkKafkaProducer09[(String, Long, Post)]("posts", postSchema, properties))
      //
      //      stream
      //        .readFile(profileInFormat, profilePath)
      //        .addSink(new FlinkKafkaProducer09[(String, Long, Profile)]("profiles", profileSchema, properties))

    }
  }
}
