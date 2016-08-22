package ru.wobot.etl
package flink.nutch.link

import java.nio.file.{Files, Paths}

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl.{search, _}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.nutch.segment.SegmentChecker
import org.apache.nutch.util.HadoopFSUtil
import org.elasticsearch.common.settings.Settings

import scala.io.Source

object EsQuery {
  def main(args: Array[String]): Unit = {
    val q: String = "путин"
    val settings = Settings.builder().put("cluster.name", "kviz-es").build()
    val client = ElasticClient.transport(settings, "elasticsearch://192.168.1.121:9300")
    val totalHintFile = "c:\\crawl\\hints.txt"
    val prevTotalHints =
      if (Files.exists(Paths.get(totalHintFile))) Integer.parseInt(Source.fromFile(totalHintFile).getLines.mkString)
      else 0

    val r = client.execute {
      search in "wobot" -> "post" query "путин" size 0
    }.await

    if (r.totalHits > prevTotalHints) {
      println("!!!!!!!!!!!!!!!")
      println(r.totalHits)
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val topScore: DataStream[Page] = env.addSource(new EsSearch(q)).startNewChain
      topScore.map(x => s"${x.url}\tnutch.score=${x.rank}").writeAsText("c:\\crawl\\es-score\\")
      env.execute()
      scala.tools.nsc.io.File(totalHintFile).writeAll(r.totalHits.toString)
    }
    else {
      generateTop(ParameterTool.fromArgs(args)).print()
    }

    //    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    val collection: DataSet[Int] = env.fromCollection(List(1, 2, 3))
    //    collection.print()
  }

  def generateTop(params: ParameterTool): DataSet[Adjacency] = {
    val fs: FileSystem = FileSystem.get(new JobConf())
    val batch = ExecutionEnvironment.getExecutionEnvironment
    val readAdjacencyListsJob = Job.getInstance()
    val segmentIn = new Path(params.getRequired("dir"))
    val segments = HadoopFSUtil.getPaths(fs.listStatus(segmentIn, HadoopFSUtil.getPassDirectoriesFilter(fs)))
    for (dir <- segments) {
      val adjacencyPath: Path = new Path(dir, "parse_adjacency")
      if (fs.exists(adjacencyPath)) {
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(readAdjacencyListsJob, adjacencyPath)
      }
    }
    val input = batch.createInput(new HadoopInputFormat[Text, Text](new SequenceFileInputFormat[Text, Text], classOf[Text], classOf[Text], readAdjacencyListsJob))
    input.map(x => JsonUtil.fromJson[Adjacency](x._2.toString))
  }

  def generateTop2(params: ParameterTool) = {
    val fs: FileSystem = FileSystem.get(new JobConf())
    try {
      if (params.has("dir")) {
        val batch = ExecutionEnvironment.getExecutionEnvironment
        val segmentIn = new Path(params.getRequired("dir"))
        val parameters = new Configuration()
        parameters.setBoolean("recursive.file.enumeration", true)
        val logs = batch.readTextFile(segmentIn.toString)
          .withParameters(parameters);
        println(logs.count())
        //        val file: DataSet[Adjacency] = batch.readFile(new TypeSerializerInputFormat[Adjacency](ajacencyTI), segmentIn.toString)
        //          .withParameters(parameters)
        //        println(file.count())

      }
    }

  }
}
