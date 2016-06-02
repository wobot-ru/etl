//package ru.wobot.etl
//
//import java.lang.Iterable
//
//import com.google.gson.Gson
//import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, GroupReduceFunction}
//import org.apache.flink.api.java.ExecutionEnvironment
//import org.apache.flink.api.java.operators._
//import org.apache.flink.api.java.tuple.{Tuple2, Tuple3}
//import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
//import org.apache.flink.core.fs.FileSystem.WriteMode
//import org.apache.flink.util.Collector
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.io.{Text, Writable}
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
//import org.apache.nutch.crawl.{CrawlDatum, Inlinks, NutchWritable}
//import org.apache.nutch.metadata.{Metadata, Nutch}
//import org.apache.nutch.parse.{ParseData, ParseText}
//import org.apache.nutch.protocol.Content
//import org.apache.nutch.segment.SegmentChecker
//import org.apache.nutch.util.{HadoopFSUtil, StringUtil}
//import ru.wobot.sm.core.mapping.{PostProperties, ProfileProperties}
//import ru.wobot.sm.core.meta.ContentMetaConstants
//import ru.wobot.sm.core.parse.ParseResult
//
//import scala.collection.JavaConverters._
//
//object NutchExportWrapOnJava {
//  def main(args: Array[String]) {
//    val startTime = System.currentTimeMillis();
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val job = org.apache.hadoop.mapreduce.Job.getInstance()
//    val format = new HadoopInputFormat[Text, Writable](new SequenceFileInputFormat[Text, Writable], classOf[Text], classOf[Writable], job)
//    val input = env.createInput(format)
//
//    val dirPath = "C:\\crawl\\dbg"
//    val dir: Path = new Path(dirPath)
//    val fs = dir.getFileSystem(job.getConfiguration);
//    val fstats = fs.listStatus(dir, HadoopFSUtil.getPassDirectoriesFilter(fs));
//    val files = HadoopFSUtil.getPaths(fstats);
//    for (p <- files)
//      if (SegmentChecker.isIndexable(p, fs)) {
//        println("Add " + p)
//        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(p, CrawlDatum.FETCH_DIR_NAME))
//        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(p, CrawlDatum.PARSE_DIR_NAME))
//        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(p, ParseData.DIR_NAME))
//        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(p, ParseText.DIR_NAME))
//      }
//
//
//    val map: FlatMapOperator[(Text, Writable), Tuple2[Text, Writable]] = input.flatMap(new FlatMapFunction[(Text, Writable), Tuple2[Text, Writable]] {
//      override def flatMap(t: (Text, Writable), collector: Collector[Tuple2[Text, Writable]]): Unit = collector.collect(Tuple2.of(t._1, t._2))
//    })
//    val map2 = map.flatMap(new FlatMapFunction[Tuple2[Text, Writable], Tuple2[Text, NutchWritable]] {
//      override def flatMap(t: Tuple2[Text, Writable], out: Collector[Tuple2[Text, NutchWritable]]): Unit = {
//        t.f1 match {
//          case datum: CrawlDatum =>
//            if (!(datum.getStatus() == CrawlDatum.STATUS_LINKED || datum.getStatus() == CrawlDatum.STATUS_SIGNATURE || datum.getStatus() == CrawlDatum.STATUS_PARSE_META))
//              out.collect(new Tuple2[Text, NutchWritable](t.f0, new NutchWritable(t.f1)))
//          case c: Content => ()
//          case i: Inlinks => ()
//          case _ => out.collect(new Tuple2[Text, NutchWritable](t.f0, new NutchWritable(t.f1)))
//        }
//      }
//    })
//
//    //println("map2=" + map2.count())
//    val group: GroupReduceOperator[Tuple2[Text, NutchWritable], Tuple3[String, Post, Profile]] = map2.groupBy(0).reduceGroup(new GroupReduceFunction[Tuple2[Text, NutchWritable], Tuple3[String, Post, Profile]] {
//      def fromJson[T](json: String, clazz: Class[T]): T = {
//        return new Gson().fromJson(json, clazz)
//      }
//
//      override def reduce(values: Iterable[Tuple2[Text, NutchWritable]], out: Collector[Tuple3[String, Post, Profile]]): Unit = {
//        var key: Text = null
//        var fetchDatum: CrawlDatum = null
//        var parseText: ParseText = null
//        var parseData: ParseData = null
//        for (elem <- values.asScala) {
//          val v = elem.f1.get()
//          key = elem.f0
//          v match {
//            case c: CrawlDatum => fetchDatum = c
//            case p: ParseData => parseData = p
//            case p: ParseText => parseText = p
//            case _ => ()
//          }
//        }
//
//        if (parseData == null) return
//        if (!parseData.getStatus.isSuccess || fetchDatum.getStatus != CrawlDatum.STATUS_FETCH_SUCCESS) return
//        val contentMeta = parseData.getContentMeta;
//        val skipFromElastic: String = contentMeta.get(ContentMetaConstants.SKIP_FROM_ELASTIC_INDEX)
//        if (skipFromElastic != null && skipFromElastic.equals("1")) return
//        val crawlDate: String = contentMeta.get(ContentMetaConstants.FETCH_TIME)
//        val segment = contentMeta.get(Nutch.SEGMENT_NAME_KEY);
//        val isSingleDoc: Boolean = !"true".equals(contentMeta.get(ContentMetaConstants.MULTIPLE_PARSE_RESULT))
//        if (isSingleDoc) {
//          val parseMeta: Metadata = parseData.getParseMeta
//          val subType = contentMeta.get(ContentMetaConstants.TYPE);
//          if (subType != null && subType.equals(ru.wobot.sm.core.mapping.Types.PROFILE)) {
//            val profile = new Profile();
//            profile.id = key.toString
//            profile.segment = segment
//            profile.crawlDate = crawlDate
//            profile.source = parseMeta.get(ProfileProperties.SOURCE)
//            profile.name = parseMeta.get(ProfileProperties.NAME)
//            profile.href = parseMeta.get(ProfileProperties.HREF)
//            profile.smProfileId = parseMeta.get(ProfileProperties.SM_PROFILE_ID)
//            profile.city = parseMeta.get(ProfileProperties.CITY)
//            profile.gender = parseMeta.get(ProfileProperties.GENDER)
//            profile.reach = parseMeta.get(ProfileProperties.REACH)
//            out.collect(Tuple3.of(key.toString(), null, profile))
//          }
//        }
//        else {
//          if (parseText == null) return
//          val content: String = parseText.getText()
//          if (StringUtil.isEmpty(content)) return
//          val parseResults: Array[ParseResult] = fromJson[Array[ParseResult]](parseText.getText, classOf[Array[ParseResult]])
//          if (parseResults != null) {
//            for (parseResult <- parseResults) {
//              var subType: String = parseResult.getContentMeta.get(ContentMetaConstants.TYPE).asInstanceOf[String]
//              if (subType == null) subType = parseData.getContentMeta.get(ContentMetaConstants.TYPE)
//              if (subType == ru.wobot.sm.core.mapping.Types.POST) {
//                val parseMeta = parseResult.getParseMeta
//                val post = new Post()
//                post.id = parseResult.getUrl
//                post.crawlDate = crawlDate
//                post.segment = segment
//                post.source = parseMeta.get(ProfileProperties.SOURCE).asInstanceOf[String]
//                post.profileId = parseMeta.get(PostProperties.PROFILE_ID).asInstanceOf[String]
//                post.href = parseMeta.get(PostProperties.HREF).asInstanceOf[String]
//                post.smPostId = String.valueOf(parseMeta.get(PostProperties.SM_POST_ID))
//                post.body = parseMeta.get(PostProperties.BODY).asInstanceOf[String]
//                post.date = parseMeta.get(PostProperties.POST_DATE).asInstanceOf[String]
//                post.isComment = parseMeta.get(PostProperties.IS_COMMENT).asInstanceOf[Boolean]
//                post.engagement = String.valueOf(parseMeta.get(PostProperties.ENGAGEMENT))
//                post.parentPostId = parseMeta.get(PostProperties.PARENT_POST_ID).asInstanceOf[String]
//                out.collect(Tuple3.of(post.id, post, null))
//              }
//            }
//          }
//        }
//      }
//    })
//
//
//    val group1: GroupReduceOperator[Tuple2[Text, NutchWritable], Tuple3[String, Post, Profile]] = group
//    val profileProj: ProjectOperator[_, Tuple2[String, Profile]] = group.project(0, 2)
//    val profiles = profileProj.filter(new FilterFunction[Tuple2[String, Profile]]() {
//      @throws[Exception]
//      def filter(tuple: Tuple2[String, Profile]): Boolean = {
//        return tuple.f1 != null
//      }
//    })
//
//    val postsProj: ProjectOperator[_, Tuple2[String, Post]] = group.project(0, 1)
//    val posts = postsProj.filter(new FilterFunction[Tuple2[String, Post]]() {
//      @throws[Exception]
//      def filter(tuple: Tuple2[String, Post]): Boolean = {
//        return tuple.f1 != null
//      }
//    })
//
//    //profiles.output(new CsvOutputFormat[Tuple2[String, Profile]](new core.fs.Path("c:\\tmp\\flink\\profile-dump"),"\n",","))
//    //    profiles.writeAsText("c:\\tmp\\flink\\profile-dump", WriteMode.OVERWRITE)
//    //    env.execute()
//    //    val postCount: Long = posts.count()
//    //    val profileCount: Long = profiles.count()
//    //    println("Total posts=" + postCount)
//    //    println("Total profiles=" + profileCount)
//    posts.writeAsCsv("c:\\tmp\\flink\\posts-dump", WriteMode.OVERWRITE)
//    profiles.writeAsCsv("c:\\tmp\\flink\\profiles-dump", WriteMode.OVERWRITE)
//
//    env.execute("Save");
//    val elapsedTime = System.currentTimeMillis() - startTime;
//    println("elapsedTime=" + elapsedTime)
//  }
//}
