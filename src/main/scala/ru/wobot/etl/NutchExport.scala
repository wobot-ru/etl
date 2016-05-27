package ru.wobot.etl

import com.google.gson.Gson
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.nutch.crawl.{CrawlDatum, NutchWritable}
import org.apache.nutch.metadata.{Metadata, Nutch}
import org.apache.nutch.parse.{ParseData, ParseText}
import org.apache.nutch.segment.SegmentChecker
import org.apache.nutch.util.{HadoopFSUtil, StringUtil}
import ru.wobot.sm.core.mapping.{PostProperties, ProfileProperties}
import ru.wobot.sm.core.meta.ContentMetaConstants
import ru.wobot.sm.core.parse.ParseResult

object NutchExport {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis();
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableForceKryo()
    val jobCrawlDatum = org.apache.hadoop.mapreduce.Job.getInstance()
    val jobParseData = org.apache.hadoop.mapreduce.Job.getInstance()

    val crawlDatumInput = env.createInput(new HadoopInputFormat[Text, CrawlDatum](new SequenceFileInputFormat[Text, CrawlDatum], classOf[Text], classOf[CrawlDatum], jobCrawlDatum))

    val parseDataInput = env.createInput(new HadoopInputFormat[Text, ParseData](new SequenceFileInputFormat[Text, ParseData], classOf[Text], classOf[ParseData], jobParseData))

    val dir = new Path(params.getRequired("segs"))
    val fs = dir.getFileSystem(jobCrawlDatum.getConfiguration);
    val files = HadoopFSUtil.getPaths(fs.listStatus(dir, HadoopFSUtil.getPassDirectoriesFilter(fs)));
    for (p <- files)
      if (SegmentChecker.isIndexable(p, fs)) {
        println("Add " + p)
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobCrawlDatum, new Path(p, CrawlDatum.FETCH_DIR_NAME))
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobCrawlDatum, new Path(p, CrawlDatum.PARSE_DIR_NAME))
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobParseData, new Path(p, ParseData.DIR_NAME))
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobParseData, new Path(p, ParseText.DIR_NAME))
      }

    val crawlMap = crawlDatumInput.flatMap((t: (Text, CrawlDatum), out: Collector[(String, NutchWritable)]) => {
      t match {
        case (id, datum) =>
          if (!(datum.getStatus() == CrawlDatum.STATUS_LINKED || datum.getStatus() == CrawlDatum.STATUS_SIGNATURE || datum.getStatus() == CrawlDatum.STATUS_PARSE_META))
            out.collect((id.toString, new NutchWritable(datum)))
      }
    })
    val parseMap = parseDataInput.flatMap((t: (Text, ParseData), out: Collector[(String, NutchWritable)]) => out.collect((t._1.toString, new NutchWritable(t._2))))

    val union: DataSet[(String, NutchWritable)] = crawlMap.union(parseMap)

    val groupBy: GroupedDataSet[(String, NutchWritable)] = union.groupBy(0)



    val map = groupBy.reduceGroup(fun = (values: Iterator[(String, NutchWritable)], out: Collector[(String, Post, Profile)]) => {
      val gson = new Gson()
      var key: String = null
      var fetchDatum: CrawlDatum = null
      var parseText: ParseText = null
      var parseData: ParseData = null

      for (elem <- values) {
        key = elem._1
        elem._2.get() match {
          case c: CrawlDatum => fetchDatum = c
          case p: ParseData => parseData = p
          case p: ParseText => parseText = p
          case _ => ()
        }
      }

      def fromJson[T](json: String, clazz: Class[T]): T = {
        return gson.fromJson(json, clazz)
      }

      if (parseData != null)
        if (parseData.getStatus.isSuccess && fetchDatum.getStatus == CrawlDatum.STATUS_FETCH_SUCCESS) {
          val contentMeta = parseData.getContentMeta;
          val skipFromElastic: String = contentMeta.get(ContentMetaConstants.SKIP_FROM_ELASTIC_INDEX)
          if (skipFromElastic == null || !skipFromElastic.equals("1")) {
            val crawlDate: String = contentMeta.get(ContentMetaConstants.FETCH_TIME)
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
                out.collect(key, null, profile)
              }
            }
            else {
              if (parseText != null) {
                val content: String = parseText.getText()
                if (!StringUtil.isEmpty(content)) {
                  val parseResults: Array[ParseResult] = fromJson[Array[ParseResult]](parseText.getText, classOf[Array[ParseResult]])
                  if (parseResults != null) {
                    for (parseResult <- parseResults) {
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
                        out.collect(post.id, post, null)
                      }
                    }
                  }
                }
              }
            }
          }
        }
    })



    val posts = map.filter(x => x._2 != null).map((tuple: (String, Post, Profile)) => (tuple._1, tuple._2))
    val profiles = map.filter(x => x._3 != null).map((tuple: (String, Post, Profile)) => (tuple._1, tuple._3))
    val unicProfiles: DataSet[(String, Profile)] = profiles.groupBy((tuple: (String, Profile)) => tuple._2.id).sortGroup((tuple: (String, Profile)) => tuple._2.crawlDate, Order.DESCENDING).first(1)

    val leftJoin = posts.leftOuterJoin(profiles).where(0).equalTo(0) {
      (left, right) =>
        val (_, post) = left
        if (right == null)
          (post.id, post, Option.empty)
        else {
          val (_, profile) = right
          (post.id, post, Option(profile))
        }
    }
    val postsWithoutProfile = leftJoin.filter(x => x._3 == Option.empty).map(x => (x._2.id, x._2))
    val postsWithProfile = leftJoin.filter(x => x._3 != Option.empty).map(x => (x._2.id, x._2, x._3.get))

    def toCsvPath(out: String) = new Path(params.getRequired("csv"), out).toString
    profiles.sortPartition(x=>x._1, Order.ASCENDING).writeAsCsv(toCsvPath("profiles"), writeMode = WriteMode.OVERWRITE)
    unicProfiles.writeAsCsv(toCsvPath("unic-profiles"), writeMode = WriteMode.OVERWRITE)
    posts.writeAsCsv(toCsvPath("posts"), writeMode = WriteMode.OVERWRITE)
    postsWithProfile.writeAsCsv(toCsvPath("posts-and-profiles"), writeMode = WriteMode.OVERWRITE)
    postsWithoutProfile.writeAsCsv(toCsvPath("posts-without-profiles"), writeMode = WriteMode.OVERWRITE)

    val profilesCount: Long = profiles.count()
    val unicProfilesCount: Long = unicProfiles.count()
    val postCount: Long = posts.count()
    val postsWithoutProfileCount: Long = postsWithoutProfile.count()
    val postsWithProfileCount: Long = postsWithProfile.count()

    println("unicProfiles.count()==" + unicProfilesCount)
    println("profiles.count()==" + profilesCount)
    println("posts.count()=" + postCount)
    println("postWithoutProfiles.count()=" + postsWithoutProfileCount)
    println("postsWithProfile.count()==" + postsWithProfileCount)
    println("postWithoutProfiles+postsWithProfile==" + (postsWithoutProfileCount + postsWithProfileCount))
    val elapsedTime = System.currentTimeMillis() - startTime
    println("elapsedTime=" + elapsedTime)
  }
}
