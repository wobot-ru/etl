//package ru.wobot.etl
//
//import com.google.gson.Gson
//import org.apache.flink.api.common.operators.Order
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
//import org.apache.flink.api.scala.{_}
//import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
//import org.apache.flink.core.fs.FileSystem.WriteMode
//import org.apache.flink.util.Collector
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.io.{Text, Writable}
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
//import org.apache.nutch.crawl.{CrawlDatum, NutchWritable}
//import org.apache.nutch.metadata.{Metadata, Nutch}
//import org.apache.nutch.parse.{ParseData, ParseText}
//import org.apache.nutch.segment.SegmentChecker
//import org.apache.nutch.util.{HadoopFSUtil, StringUtil}
//import ru.wobot.sm.core.mapping.{PostProperties, ProfileProperties}
//import ru.wobot.sm.core.meta.ContentMetaConstants
//import ru.wobot.sm.core.parse.ParseResult
//
//
//object NutchExport3 {
//  def main(args: Array[String]): Unit = {
//    val startTime = System.currentTimeMillis()
//    val params: ParameterTool = ParameterTool.fromArgs(args)
//    //    val conf = new org.apache.flink.configuration.Configuration();
//    //    conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS * 2);
//    //    val env = ExecutionEnvironment.createLocalEnvironment(conf)
//
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.enableForceKryo()
//    //env.getConfig.enableClosureCleaner()
//
//    val jobCrawlDatum = org.apache.hadoop.mapreduce.Job.getInstance()
//
//
//    val segmentIn = new Path(params.getRequired("segs"))
//    val fs = segmentIn.getFileSystem(jobCrawlDatum.getConfiguration)
//    val segments = HadoopFSUtil.getPaths(fs.listStatus(segmentIn, HadoopFSUtil.getPassDirectoriesFilter(fs)))
//    for (dir <- segments)
//      if (SegmentChecker.isIndexable(dir, fs)) {
//        if (SegmentChecker.isIndexable(dir, fs)) {
//          println("Add " + dir)
//          org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobCrawlDatum, new Path(dir, CrawlDatum.FETCH_DIR_NAME))
//          org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobCrawlDatum, new Path(dir, CrawlDatum.PARSE_DIR_NAME))
//          org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobCrawlDatum, new Path(dir, ParseData.DIR_NAME))
//          org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobCrawlDatum, new Path(dir, ParseText.DIR_NAME))
//        }
//
//      }
//
//    val crawlDatumInput = env.createInput(new HadoopInputFormat[Text, NutchWritable](new SequenceFileInputFormat[Text, NutchWritable], classOf[Text], classOf[NutchWritable], jobCrawlDatum))
//    val crawlMap = crawlDatumInput.flatMap((t: (Text, Writable), out: Collector[(Text, NutchWritable)]) => {
//      t._2 match {
//        case c: CrawlDatum => if (c.getStatus == CrawlDatum.STATUS_FETCH_SUCCESS) out.collect((t._1, new NutchWritable(t._2)))
//        case c: ParseData => if (c.getStatus.isSuccess) out.collect((t._1, new NutchWritable(t._2)))
//        case c: ParseText => out.collect((t._1, new NutchWritable(t._2)))
//      }
//
//    })
//
//
//    val map = crawlMap.groupBy(0).reduceGroup((tuples: Iterator[(Text, NutchWritable)], out: Collector[(String, Long, Option[Post], Option[Profile])]) => {
//      val gson = new Gson()
//      def fromJson[T](json: String, clazz: Class[T]): T = {
//        return gson.fromJson(json, clazz)
//      }
//
//      var key: String = null
//      var fetchDatum: CrawlDatum = null
//      var parseData: ParseData = null
//      var parseText: ParseText = null
//      val dic = collection.mutable.Map[String, NutchWritableContainer]()
//
//      val toArray: Array[(Text, NutchWritable)] = tuples.toArray
//      for ((url, data) <- toArray) {
//        data.get() match {
//          case c: CrawlDatum => {
//            key = url.toString
//            fetchDatum = c
//          }
//          case d: ParseData => parseData = d
//          case t: ParseText => parseText = t
//          case _ => ()
//        }
//      }
//      if (parseData != null && fetchDatum != null) {
//        val contentMeta = parseData.getContentMeta
//        val skipFromElastic: String = contentMeta.get(ContentMetaConstants.SKIP_FROM_ELASTIC_INDEX)
//        if (skipFromElastic == null || !skipFromElastic.equals("1")) {
//          val fetchTime: Long = fetchDatum.getFetchTime
//          val crawlDate: String = fetchTime.toString
//          val segment = contentMeta.get(Nutch.SEGMENT_NAME_KEY);
//          val isSingleDoc: Boolean = !"true".equals(contentMeta.get(ContentMetaConstants.MULTIPLE_PARSE_RESULT))
//          if (isSingleDoc) {
//            val parseMeta: Metadata = parseData.getParseMeta
//            val subType = contentMeta.get(ContentMetaConstants.TYPE);
//            if (subType != null && subType.equals(ru.wobot.sm.core.mapping.Types.PROFILE)) {
//              val profile = new Profile();
//              profile.id = key
//              profile.segment = segment
//              profile.crawlDate = crawlDate
//              profile.source = parseMeta.get(ProfileProperties.SOURCE)
//              profile.name = parseMeta.get(ProfileProperties.NAME)
//              profile.href = parseMeta.get(ProfileProperties.HREF)
//              profile.smProfileId = parseMeta.get(ProfileProperties.SM_PROFILE_ID)
//              profile.city = parseMeta.get(ProfileProperties.CITY)
//              profile.gender = parseMeta.get(ProfileProperties.GENDER)
//              profile.reach = parseMeta.get(ProfileProperties.REACH)
//              //if (crawlDate != null)
//              out.collect(profile.id, fetchTime, None, Some(profile))
//            }
//          }
//          else if (parseText != null) {
//            val content: String = parseText.getText()
//            if (!StringUtil.isEmpty(content)) {
//              val parseResults: Array[ParseResult] = fromJson[Array[ParseResult]](parseText.getText, classOf[Array[ParseResult]])
//              if (parseResults != null) for (parseResult <- parseResults) {
//                var subType: String = parseResult.getContentMeta.get(ContentMetaConstants.TYPE).asInstanceOf[String]
//                if (subType == null) subType = parseData.getContentMeta.get(ContentMetaConstants.TYPE)
//                if (subType == ru.wobot.sm.core.mapping.Types.POST) {
//                  val parseMeta = parseResult.getParseMeta
//                  val post = new Post()
//                  post.id = parseResult.getUrl
//                  post.crawlDate = crawlDate
//                  post.segment = segment
//                  post.source = parseMeta.get(ProfileProperties.SOURCE).asInstanceOf[String]
//                  post.profileId = parseMeta.get(PostProperties.PROFILE_ID).asInstanceOf[String]
//                  post.href = parseMeta.get(PostProperties.HREF).asInstanceOf[String]
//                  post.smPostId = String.valueOf(parseMeta.get(PostProperties.SM_POST_ID)).replace(".0", "")
//                  post.body = parseMeta.get(PostProperties.BODY).asInstanceOf[String]
//                  post.date = parseMeta.get(PostProperties.POST_DATE).asInstanceOf[String]
//                  post.isComment = parseMeta.get(PostProperties.IS_COMMENT).asInstanceOf[Boolean]
//                  post.engagement = String.valueOf(parseMeta.get(PostProperties.ENGAGEMENT)).replace(".0", "")
//                  post.parentPostId = parseMeta.get(PostProperties.PARENT_POST_ID).asInstanceOf[String]
//                  //if (crawlDate != null)
//                  out.collect(post.id, fetchTime, Some(post), None)
//                }
//              }
//            }
//          }
//        }
//      }
//    })
//
//    val posts = map.filter(x => x._3.isDefined).map((tuple: (String, Long, Option[Post], Option[Profile])) => (tuple._1, tuple._2, tuple._3.get))
//    val profiles = map.filter(x => x._4.isDefined).map((tuple: (String, Long, Option[Post], Option[Profile])) => (tuple._1, tuple._2, tuple._4.get))
//    //    val posts = postList.reduce((a, b) => a.union(b)).distinct(0, 1)
//    //    val profiles = profileList.reduce((a, b) => a.union(b)).distinct(0, 1)
//    //val latestProfiles: DataSet[(String, Profile)] = profiles.groupBy((tuple: (String, Profile)) => tuple._1).sortGroup((tuple: (String, Profile)) => tuple._2.crawlDate, Order.DESCENDING).first(1)
//    val latestProfiles: DataSet[(String, Profile)] = profiles.groupBy(_._1).sortGroup(_._2, Order.DESCENDING).first(1).map(x => (x._1, x._3))
//    //val latestPosts: DataSet[(String, Post)] = posts.groupBy((tuple: (String, Post)) => tuple._1).sortGroup((tuple: (String, Post)) => tuple._2.crawlDate, Order.DESCENDING).first(1).rebalance()
//    val latestPosts: DataSet[(String, Post)] = posts.groupBy(_._1).sortGroup(_._2, Order.DESCENDING).first(1).map(x => (x._3.profileId, x._3))
//
//    val leftJoin = latestPosts.leftOuterJoin(latestProfiles).where(0).equalTo(0) {
//      (left, right) =>
//        val (_, post) = left
//        if (right == null)
//          (post.id, post, None)
//        else {
//          val (_, profile) = right
//          (post.id, post, Option(profile))
//        }
//    }.sortPartition(_._1, Order.ASCENDING)
//    val postsWithoutProfile = leftJoin.filter(x => x._3.isEmpty).map(x => (x._1, x._2))
//    val postsWithProfile = leftJoin.filter(x => x._3.isDefined).map(x => (x._1, x._2, x._3.get))
//
//    def toCsvPath(out: String) = new Path(params.getRequired("csv"), out).toString
//    profiles.sortPartition(_._1, Order.ASCENDING).writeAsCsv(toCsvPath("profiles"), writeMode = WriteMode.OVERWRITE)
//    //latestProfiles.sortPartition(_._1, Order.ASCENDING).writeAsCsv(toCsvPath("latest-profiles"), writeMode = WriteMode.OVERWRITE)
//    //latestPosts.sortPartition(_._1, Order.ASCENDING).writeAsCsv(toCsvPath("latest-posts"), writeMode = WriteMode.OVERWRITE)
//    //posts.sortPartition(_._1, Order.ASCENDING).writeAsCsv(toCsvPath("posts"), writeMode = WriteMode.OVERWRITE)
//    postsWithProfile.writeAsCsv(toCsvPath("posts-and-profiles"), writeMode = WriteMode.OVERWRITE)
//    postsWithoutProfile.writeAsCsv(toCsvPath("posts-without-profiles"), writeMode = WriteMode.OVERWRITE)
//
//    env.execute("Save to cvs")
//
//    //    val profilesCount: Long = profiles.count()
//    //    val latestProfilesCount: Long = latestProfiles.count()
//    //    //val latestPostsCount: Long = latestPosts.mapCount()
//    //    //val postCount: Long = posts.mapCount()
//    //    val postsWithoutProfileCount: Long = postsWithoutProfile.count()
//    //    val postsWithProfileCount: Long = postsWithProfile.count()
//    //
//    //    println("latestProfiles.mapCount()==" + latestProfilesCount)
//    //    //println("latestPostsCount.mapCount()==" + latestPostsCount)
//    //    println("profiles.mapCount()==" + profilesCount)
//    //    //println("posts.mapCount()=" + postCount)
//    //    println("postWithoutProfiles.mapCount()=" + postsWithoutProfileCount)
//    //    println("postsWithProfile.mapCount()==" + postsWithProfileCount)
//    //    println("postWithoutProfiles+postsWithProfile==" + (postsWithoutProfileCount + postsWithProfileCount))
//
//    val elapsedTime = System.currentTimeMillis() - startTime
//    println("elapsedTime=" + elapsedTime)
//  }
//
//}
