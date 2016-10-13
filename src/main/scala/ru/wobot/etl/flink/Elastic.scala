package ru.wobot.etl.flink

import java.net.{InetAddress, InetSocketAddress}
import java.security.InvalidParameterException
import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch2.{ElasticsearchSink, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.hadoop.fs.{FileSystem, InvalidPathException, Path}
import org.apache.hadoop.mapred.JobConf
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}
import ru.wobot.etl.flink.Params._
import ru.wobot.etl.{DetailedPost, _}

import scala.collection.JavaConverters._

object Elastic {
  private val LOGGER: Logger = LoggerFactory.getLogger(HBase.getClass.getName)

  def main(args: Array[String]): Unit = {
    LOGGER.info("Run hbase")

    val params = ParameterTool.fromArgs(args)

    val outDir = params.getRequired(HBASE_OUT_DIR)
    if (!params.has(HBASE_EXPORT) && (!params.has(UPLOAD_TO_ES)))
      throw new IllegalArgumentException("Missed parameter: \"--hbase-export\" or \"--upload-to-es\"")

    if (params.has(HBASE_EXPORT)) {
      val env = ExecutionEnvironment.getExecutionEnvironment
      env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 120000))
      env.getConfig.enableForceKryo()

      val input: DataSet[DetailedPost] = env.createInput(InputFormat.postToEs()).rebalance().name("post-to-es")
      val notEmpty = input.filter(element => element.post.body != null && !element.post.body.isEmpty)

      notEmpty.write(new TypeSerializerOutputFormat[DetailedPost], outDir, WriteMode.OVERWRITE).name(s"export-to: $outDir")
      env.execute("export-from-hbase-to-filesystem")
    }

    if (params.has(UPLOAD_TO_ES)) {
      val transports = params
        .getRequired(ES_HOSTS)
        .split(',')
        .map(_.split(':') match {
          case Array(host, port) => new InetSocketAddress(InetAddress.getByName(host), port.toInt)
          case Array(host) => new InetSocketAddress(InetAddress.getByName(host), 9300)
          case _ => throw new InvalidParameterException()
        }).toList

      val fs = FileSystem.get(new JobConf())
      if (!fs.exists(new Path(outDir))) throw new InvalidPathException(s"Path not exist: $outDir")

      val esConfig: util.Map[String, String] = new util.HashMap[String, String]()
      esConfig.put("cluster.name", "wobot-new-cluster")
      esConfig.put("bulk.flush.max.size.mb", "5")
      //esConfig.put("bulk.flush.max.actions", "1000")
      //ParameterTool.fromPropertiesFile(getClass.getClassLoader.getResource("es.properties").getFile).toMap

      val stream = StreamExecutionEnvironment.getExecutionEnvironment
      stream.getConfig.enableForceKryo()

      /*val s1 = "{\"profileCity\":\"Moscow\",\"profileGender\":null,\"profileHref\":\"https://www.facebook.com/Softline.events/\",\"profileName\":\"IT-мероприятия\",\"reach\":\"2630\",\"smProfileId\":\"146214462119256\",\"profileId\":\"fb://146214462119256\",\"smPostId\":\"146214462119256_1027746140632746\",\"parentPostId\":null,\"body\":\"Коллеги, приглашаем на конференцию Google Cloud Day, которая состоится 21 апреля в Москве!\\nПодробности и регистрация по ссылке: http://events.softline.ru/event/google_cloud_day_383 Первый в этом году семинар, посвященный облачным сервисам Google Apps for Work состоится в Москве 21 апреля 2016!\\n\\nGoogle Cloud Day - конференция для руководителей эффективного бизнеса.\\nЭффективность бизнеса - цель любого руководителя. Что под ней понимать решает каждый сам для себя. Но одно понятно наверняка - слаженная работа бизнес-команд это ключ к успеху.\\nЖдем вас!\",\"date\":\"2016-04-11T15:19:22.000+0300\",\"engagement\":\"0\",\"isComment\":false,\"id\":\"fb://146214462119256/posts/146214462119256_1027746140632746\",\"segment\":\"20160817143312\",\"crawlDate\":\"1471434127594\",\"href\":\"https://www.facebook.com/146214462119256/posts/1027746140632746\",\"source\":\"fb\"}"
      /*val s2 = "{\"profileCity\":\"Санкт-Петербург\",\"profileGender\":\"M\",\"profileHref\":\"http://vk.com/durov\",\"profileName\":\"Павел Дуров\",\"reach\":\"6070239\",\"smProfileId\":\"1\",\"profileId\":\"vk://id1\",\"smPostId\":\"1023110\",\"parentPostId\":null,\"body\":\" в первой десятке – 5 команд из России, 2 из Польши, одна из Китая и две из США.  \\n\\nТак как обе команды из США на 100% состоят из этнических\",\"date\":\"2000-12-21T02:29:27+0000\",\"engagement\":\"25738\",\"isComment\":false,\"id\":\"vk://id1/posts/1023110\",\"segment\":\"20160611004543\",\"crawlDate\":\"1465596228029\",\"href\":\"http://vk.com/wall1_1023110\",\"source\":\"vk\"}"
      val s3 = "{\"profileCity\":\"Санкт-Петербург\",\"profileGender\":\"M\",\"profileHref\":\"http://vk.com/durov\",\"profileName\":\"Павел Дуров\",\"reach\":\"6070239\",\"smProfileId\":\"1\",\"profileId\":\"vk://id1\",\"smPostId\":\"1023110\",\"parentPostId\":null,\"body\":\"в первой десятке – 5 команд из России, 2 из Польши, одна из Китая и две из США.  \\n\\nТак как обе команды из США на 100% состоят из этнических \",\"date\":\"2015-07-21T02:29:27.000+0300\",\"engagement\":\"25738\",\"isComment\":false,\"id\":\"vk://id1/posts/1023110\",\"segment\":\"20160611004543\",\"crawlDate\":\"1465596228029\",\"href\":\"http://vk.com/wall1_1023110\",\"source\":\"vk\"}"
      val s4 = "{\"profileCity\":\"Санкт-Петербург\",\"profileGender\":\"M\",\"profileHref\":\"http://vk.com/durov\",\"profileName\":\"Павел Дуров\",\"reach\":\"6070239\",\"smProfileId\":\"1\",\"profileId\":\"vk://id1\",\"smPostId\":\"1023110\",\"parentPostId\":null,\"body\":\"в первой десятке – 5 команд из России, 2 из Польши, одна из Китая и две из США.  \\n\\nТак как обе кома\",\"date\":\"2015-03-21T02:29:27.000+0300\",\"engagement\":\"25738\",\"isComment\":false,\"id\":\"vk://id1/posts/1023110\",\"segment\":\"20160611004543\",\"crawlDate\":\"1465596228029\",\"href\":\"http://vk.com/wall1_1023110\",\"source\":\"vk\"}"*/

      val tmp = List(new DetailedPost("fb://146214462119256/posts/146214462119256_1027746140632746", 1465596228029L, JsonUtil.fromJson[DetailedPostDto](s1)) /*,
        new DetailedPost("vk://id1/posts/1023110", 1465596228029L, JsonUtil.fromJson[DetailedPostDto](s2)) ,
        new DetailedPost("vk://id1/posts/1023110", 1465596228029L, JsonUtil.fromJson[DetailedPostDto](s3)),
        new DetailedPost("vk://id1/posts/1023110", 1465596228029L, JsonUtil.fromJson[DetailedPostDto](s4))*/
      )

      val posts = stream.fromCollection(tmp)*/

      val posts: DataStream[DetailedPost] = stream
        .readFile(new TypeSerializerInputFormat[DetailedPost](detailedPostTI), outDir)

      val notOldPosts = posts.filter(element => {
        val postDate = ZonedDateTime.parse(element.post.date.replace(".000", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZZ"))
        postDate.getYear >= 2014
      })

      notOldPosts.addSink(new ElasticsearchSink(esConfig, transports.asJava, new ElasticsearchSinkFunction[DetailedPost] {
        override def process(post: DetailedPost, ctx: RuntimeContext, indexer: RequestIndexer) {
          val postDate = ZonedDateTime.parse(post.post.date.replace(".000", ""), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZZ"))
          val utcDate = postDate.withZoneSameInstant(ZoneOffset.UTC)
          val indexName = "wobot-monthly-y" + utcDate.format(DateTimeFormatter.ofPattern("yyyy'-hy-q'q'-m'M"))
          LOGGER.info(indexName.replace("hy", if (utcDate.getMonthValue > 6) "hy2" else "hy1"))
          indexer.add(Requests.indexRequest.index(indexName.replace("hy", if (utcDate.getMonthValue > 6) "hy2" else "hy1"))
            .`type`("post")
            .source(post.post.toJson)
            .id(post.url))
        }
      })
      )

      stream.execute("upload-post-to-es")
    }
  }

}
