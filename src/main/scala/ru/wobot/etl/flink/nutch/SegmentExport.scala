package ru.wobot.etl.flink.nutch

import com.google.gson.Gson
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
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
import ru.wobot.etl.{JsonUtil, Post, Profile}
import ru.wobot.sm.core.mapping.{PostProperties, ProfileProperties}
import ru.wobot.sm.core.meta.ContentMetaConstants
import ru.wobot.sm.core.parse.ParseResult

//import org.apache.flink.api.scala.typeutils._

object SegmentExport {
  val batch = ExecutionEnvironment.getExecutionEnvironment
  val stream = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {
    batch.getConfig.enableForceKryo()
    stream.getConfig.enableForceKryo()

    val startTime = System.currentTimeMillis()
    val params: ParameterTool = ParameterTool.fromArgs(args)

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
    //stream.execute("Publish to kafka...")
    try {
      batch.execute("Exporting data from segments...")

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

  implicit val information = createTypeInformation[(String, Long, String)].asInstanceOf[CaseClassTypeInfo[(String, Long, String)]]

  def addSegment(segmentPath: Path): Unit = {
    val exportJob = org.apache.hadoop.mapreduce.Job.getInstance()
    val fs = segmentPath.getFileSystem(exportJob.getConfiguration);
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
                val profile = new Profile();
                profile.id = key
                profile.segment = segment
                profile.crawlDate = crawlDate
                profile.source = parseMeta.get(ProfileProperties.SOURCE)
                profile.name = parseMeta.get(ProfileProperties.NAME)
                profile.href = parseMeta.get(ProfileProperties.HREF)
                profile.smProfileId = parseMeta.get(ProfileProperties.SM_PROFILE_ID)
                profile.city = parseMeta.get(ProfileProperties.CITY)
                profile.gender = parseMeta.get(ProfileProperties.GENDER)
                profile.reach = parseMeta.get(ProfileProperties.REACH)
                //if (crawlDate != null)
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
                    val post = new Post()
                    post.id = parseResult.getUrl
                    post.crawlDate = crawlDate
                    post.segment = segment
                    post.source = parseMeta.get(ProfileProperties.SOURCE).asInstanceOf[String]
                    post.profileId = parseMeta.get(PostProperties.PROFILE_ID).asInstanceOf[String]
                    post.href = parseMeta.get(PostProperties.HREF).asInstanceOf[String]
                    post.smPostId = String.valueOf(parseMeta.get(PostProperties.SM_POST_ID)).replace(".0", "")
                    post.body = parseMeta.get(PostProperties.BODY).asInstanceOf[String]
                    post.date = parseMeta.get(PostProperties.POST_DATE).asInstanceOf[String]
                    post.isComment = parseMeta.get(PostProperties.IS_COMMENT).asInstanceOf[Boolean]
                    post.engagement = String.valueOf(parseMeta.get(PostProperties.ENGAGEMENT)).replace(".0", "")
                    post.parentPostId = parseMeta.get(PostProperties.PARENT_POST_ID).asInstanceOf[String]
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

      val unic: DataSet[(String, Long, Option[Post], Option[Profile])] = data.distinct(0, 1)
      val posts = unic.filter(x => x._3.isDefined).map((tuple: (String, Long, Option[Post], Option[Profile])) => (tuple._1, tuple._2, JsonUtil.toJson(tuple._3.get))).sortPartition(0, Order.ASCENDING)
      val profiles = unic.filter(x => x._4.isDefined).map((tuple: (String, Long, Option[Post], Option[Profile])) => (tuple._1, tuple._2,  JsonUtil.toJson(tuple._4.get))).sortPartition(0, Order.ASCENDING)

      posts.write(new TypeSerializerOutputFormat[(String, Long, String)], postPath, WriteMode.OVERWRITE)
      profiles.write(new TypeSerializerOutputFormat[(String, Long, String)], profilePath, WriteMode.OVERWRITE)
      //      posts.writeAsCsv(postPath, writeMode = WriteMode.OVERWRITE)
      //      profiles.writeAsCsv(profilePath, writeMode = WriteMode.OVERWRITE)

      //val format = new TupleCsvInputFormat[(String, Long, String)](new org.apache.flink.core.fs.Path(profilePath), information)
      //val profileStream = stream.createInput(format)
      //val profileStream2: DataStream[(String, Long, Profile)] = stream.createInput(new TypeSerializerInputFormat[(String, Long, Profile)](TypeInformation.of(classOf[(String, Long, Profile)])))
//      val profileStream: DataStream[(String, Long, Profile)] = stream.readFile(new TypeSerializerInputFormat[(String, Long, Profile)](TypeInformation.of(classOf[(String, Long, Profile)])), profilePath)
//      profileStream.print()
//      val out: String = s"file:////c:\\tmp\\flink\\${segmentPath.getName}\\stream-out"
//      val outputFormat: TypeSerializerOutputFormat[(String, Long, Profile)] = new TypeSerializerOutputFormat[(String, Long, Profile)]
//      outputFormat.setOutputFilePath(new core.fs.Path(out))
//      profileStream.writeUsingOutputFormat(outputFormat)
      //      println(s"Add ${postCsv.getDataSet.count()}  posts")
      //      println(s"Add ${profileCsv.getDataSet.count()}  profiles")
    }
  }
}
