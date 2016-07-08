package ru.wobot.etl.flink

import java.net.{InetAddress, InetSocketAddress}
import java.security.InvalidParameterException

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
      input.write(new TypeSerializerOutputFormat[DetailedPost], outDir, WriteMode.OVERWRITE).name(s"export-to: $outDir")
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

      val indexName = params.getRequired(INDEX_NAME)
      val fs = FileSystem.get(new JobConf())
      if (!fs.exists(new Path(outDir))) throw new InvalidPathException(s"Path not exist: $outDir")

      val esConfig = ParameterTool.fromPropertiesFile(getClass.getClassLoader.getResource("es.properties").getFile).toMap

      val stream = StreamExecutionEnvironment.getExecutionEnvironment
      stream.getConfig.enableForceKryo()
      val posts: DataStream[DetailedPost] = stream
        .readFile(new TypeSerializerInputFormat[DetailedPost](detailedPostTI), outDir)

      posts.addSink(new ElasticsearchSink(esConfig, transports.asJava, new ElasticsearchSinkFunction[DetailedPost] {
        override def process(post: DetailedPost, ctx: RuntimeContext, indexer: RequestIndexer) {
          indexer.add(Requests.indexRequest.index(indexName).`type`("post").source(post.post.toJson()).id(post.url))
        }
      })
      )

      stream.execute("upload-post-to-es")
    }
  }

}
