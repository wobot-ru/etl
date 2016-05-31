package ru.wobot.etl

import com.google.gson.Gson
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.configuration.ConfigConstants
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

import scala.collection.mutable.ListBuffer

object NutchExport2 {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val params: ParameterTool = ParameterTool.fromArgs(args)
    //    val conf = new org.apache.flink.configuration.Configuration();
    //    conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS * 2);
    //    val env = ExecutionEnvironment.createLocalEnvironment(conf)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableForceKryo()
    //env.getConfig.enableClosureCleaner()

    val jobCrawlDatum = org.apache.hadoop.mapreduce.Job.getInstance()
    val jobParseData = org.apache.hadoop.mapreduce.Job.getInstance()
    val jobTextInput = org.apache.hadoop.mapreduce.Job.getInstance()

    val crawlDatumInput = env.createInput(new HadoopInputFormat[Text, CrawlDatum](new SequenceFileInputFormat[Text, CrawlDatum], classOf[Text], classOf[CrawlDatum], jobCrawlDatum))
    val parseDataInput = env.createInput(new HadoopInputFormat[Text, ParseData](new SequenceFileInputFormat[Text, ParseData], classOf[Text], classOf[ParseData], jobParseData))
    val parseTextInput = env.createInput(new HadoopInputFormat[Text, ParseText](new SequenceFileInputFormat[Text, ParseText], classOf[Text], classOf[ParseText], jobTextInput))

    val segmentIn = new Path(params.getRequired("segs"))
    val fs = segmentIn.getFileSystem(jobCrawlDatum.getConfiguration);
    val segments = HadoopFSUtil.getPaths(fs.listStatus(segmentIn, HadoopFSUtil.getPassDirectoriesFilter(fs)))
    //val list = new ListBuffer[DataSet[((String, String), Option[Post], Option[Profile])]]()
    val profileList = new ListBuffer[DataSet[(String, String, Profile)]]()
    val postList = new ListBuffer[DataSet[(String, String, Post)]]()
    for (dir <- segments)
      if (SegmentChecker.isIndexable(dir, fs)) {
        if (SegmentChecker.isIndexable(dir, fs)) {
          println("Add " + dir)
          org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobCrawlDatum, new Path(dir, CrawlDatum.FETCH_DIR_NAME))
          org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobCrawlDatum, new Path(dir, CrawlDatum.PARSE_DIR_NAME))
          org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobParseData, new Path(dir, ParseData.DIR_NAME))
          org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobTextInput, new Path(dir, ParseText.DIR_NAME))
        }


        val crawlMap = crawlDatumInput.flatMap((t: (Text, CrawlDatum), out: Collector[(String, NutchWritable)]) => {
          val (id, datum) = t
          if (!(datum.getStatus() == CrawlDatum.STATUS_LINKED || datum.getStatus() == CrawlDatum.STATUS_SIGNATURE || datum.getStatus() == CrawlDatum.STATUS_PARSE_META))
            out.collect((id.toString, new NutchWritable(datum)))
        })
        val parseMap = parseDataInput.flatMap((t: (Text, ParseData), out: Collector[(String, NutchWritable)]) => out.collect((t._1.toString, new NutchWritable(t._2))))
        val textMap = parseTextInput.flatMap((t: (Text, ParseText), out: Collector[(String, NutchWritable)]) => out.collect((t._1.toString, new NutchWritable(t._2))))

        val u = crawlMap.union(parseMap).union(textMap)

        val map = u.groupBy(0).reduceGroup((tuples: Iterator[(String, NutchWritable)], out: Collector[(String, String, Option[Post], Option[Profile])]) => {
          val gson = new Gson()
          def fromJson[T](json: String, clazz: Class[T]): T = {
            return gson.fromJson(json, clazz)
          }

          var key: String = null
          var fetchDatum: CrawlDatum = null
          var parseData: ParseData = null
          var parseText: ParseText = null

          for ((url, data) <- tuples) {
            key = url
            data.get() match {
              case c: CrawlDatum => fetchDatum = c
              case d: ParseData => parseData = d
              case t: ParseText => parseText = t
              case _ => ()
            }
          }
          if ((parseData != null && fetchDatum != null) && parseData.getStatus.isSuccess && fetchDatum.getStatus == CrawlDatum.STATUS_FETCH_SUCCESS) {
            val contentMeta = parseData.getContentMeta;
            val skipFromElastic: String = contentMeta.get(ContentMetaConstants.SKIP_FROM_ELASTIC_INDEX)
            if (skipFromElastic == null || !skipFromElastic.equals("1")) {
              val crawlDate: String = fetchDatum.getFetchTime.toString
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
                  if (profile.id != null && crawlDate != null)
                    out.collect(profile.id, crawlDate, None, Some(profile))
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
                      if (post.id != null && crawlDate != null)
                        out.collect(post.id, crawlDate, Some(post), None)
                    }
                  }
                }
              }
            }
          }
        })
        postList += map.filter(x => x._3.isDefined).map((tuple: (String, String, Option[Post], Option[Profile])) => (tuple._1, tuple._2, tuple._3.get))
        profileList += map.filter(x => x._4.isDefined).map((tuple: (String, String, Option[Post], Option[Profile])) => (tuple._1, tuple._2, tuple._4.get))
      }


    val posts = postList.reduce((a, b) => a.union(b))
    val profiles = profileList.reduce((a, b) => a.union(b))
    //val latestProfiles: DataSet[(String, Profile)] = profiles.groupBy((tuple: (String, Profile)) => tuple._1).sortGroup((tuple: (String, Profile)) => tuple._2.crawlDate, Order.DESCENDING).first(1)
    val latestProfiles: DataSet[(String, Profile)] = profiles.groupBy(_._1).sortGroup(_._2, Order.DESCENDING).first(1).map(x => (x._1, x._3))
    //val latestPosts: DataSet[(String, Post)] = posts.groupBy((tuple: (String, Post)) => tuple._1).sortGroup((tuple: (String, Post)) => tuple._2.crawlDate, Order.DESCENDING).first(1).rebalance()
    val latestPosts: DataSet[(String, Post)] = posts.groupBy(_._1).sortGroup(_._2, Order.DESCENDING).first(1).map(x => (x._1, x._3))

    val leftJoin = latestPosts.leftOuterJoin(latestProfiles).where(0).equalTo(0) {
      (left, right) =>
        val (_, post) = left
        if (right == null)
          (post.id, post, Option.empty)
        else {
          val (_, profile) = right
          (post.id, post, Option(profile))
        }
    }
    val postsWithoutProfile = leftJoin.filter(x => x._3.isEmpty).map(x => (x._1, x._2))
    val postsWithProfile = leftJoin.filter(x => x._3.isDefined).map(x => (x._1, x._2, x._3.get))

    def toCsvPath(out: String) = new Path(params.getRequired("csv"), out).toString
    //profiles.writeAsCsv(toCsvPath("profiles"), writeMode = WriteMode.OVERWRITE)
    latestProfiles.writeAsCsv(toCsvPath("latest-profiles"), writeMode = WriteMode.OVERWRITE)
    //latestPosts.writeAsCsv(toCsvPath("latest-posts"), writeMode = WriteMode.OVERWRITE)
    //posts.writeAsCsv(toCsvPath("posts"), writeMode = WriteMode.OVERWRITE)
    postsWithProfile.writeAsCsv(toCsvPath("posts-and-profiles"), writeMode = WriteMode.OVERWRITE)
    postsWithoutProfile.writeAsCsv(toCsvPath("posts-without-profiles"), writeMode = WriteMode.OVERWRITE)

    env.execute("Save to cvs")

    //    val profilesCount: Long = profiles.count()
    //    val latestProfilesCount: Long = latestProfiles.count()
    //    //val latestPostsCount: Long = latestPosts.count()
    //    //val postCount: Long = posts.count()
    //    val postsWithoutProfileCount: Long = postsWithoutProfile.count()
    //    val postsWithProfileCount: Long = postsWithProfile.count()
    //
    //    println("latestProfiles.count()==" + latestProfilesCount)
    //    //println("latestPostsCount.count()==" + latestPostsCount)
    //    //println("profiles.count()==" + profilesCount)
    //    //println("posts.count()=" + postCount)
    //    println("postWithoutProfiles.count()=" + postsWithoutProfileCount)
    //    println("postsWithProfile.count()==" + postsWithProfileCount)
    //    println("postWithoutProfiles+postsWithProfile==" + (postsWithoutProfileCount + postsWithProfileCount))

    val elapsedTime = System.currentTimeMillis() - startTime
    println("elapsedTime=" + elapsedTime)
  }

}
