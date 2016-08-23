package ru.wobot.etl
package flink.nutch.link

import java.io.FileWriter
import java.nio.file.{Files, Paths}
import java.util

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl.{search, _}
import org.apache.flink.api.common.functions.{GroupReduceFunction, MapPartitionFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.nutch.util.HadoopFSUtil
import org.elasticsearch.common.settings.Settings

import scala.collection.mutable
import scala.io.Source
import scala.math.Ordering

object EsQuery {
  val EOL = System.getProperty("line.separator")
  val ES_TOP_SCORE_PATH = "C:\\crawl\\focus\\es-top-score\\"
  val TOTAL_HINT_FILE_PATH = "C:\\crawl\\focus\\hints.txt"
  val INJECT_DIR_PATH = "C:\\crawl\\focus\\to-inject\\"
  val q: String = "путин"
  val TOP_N = 1500
  val batch = ExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    val settings = Settings.builder().put("cluster.name", "kviz-es").build()
    val client = ElasticClient.transport(settings, "elasticsearch://192.168.1.121:9300")
    val prevTotalHints =
      if (Files.exists(Paths.get(TOTAL_HINT_FILE_PATH))) Integer.parseInt(Source.fromFile(TOTAL_HINT_FILE_PATH).getLines.mkString)
      else 0

    val r = client.execute {
      search in "wobot" -> "post" query q size 0
    }.await

    if (r.totalHits > prevTotalHints) {
      println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   MATCH  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   QUERY  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      println(s"\t\t${r.totalHits} > $prevTotalHints")
      println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      val adjacency = getAdjacencyDS(ParameterTool.fromArgs(args))
      val tops = getTopPageDS()
      val pg = pageRank(adjacency, tops)

      val unfetched: DataSet[Page] = pg.filter(x => x.crawlDate <= 0)

      val top: Long = unfetched.count() / 100
      println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      println(s"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   $top  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      topN(unfetched, top).map(_.url).writeAsText(new Path(INJECT_DIR_PATH, "seeds.txt").toString, WriteMode.OVERWRITE)
      //unfetched.map(_.url).first(TOP_N).writeAsText(new Path(INJECT_DIR_PATH, "seeds.txt").toString, WriteMode.OVERWRITE)
      batch.execute()
      scala.tools.nsc.io.File(TOTAL_HINT_FILE_PATH).writeAll(r.totalHits.toString)
    }
  }

  def pageRank(adjacency: DataSet[Adjacency], tops: DataSet[Page]): DataSet[Page] = {
    val maxIterations = 100
    val DAMPENING_FACTOR: Double = 0.85
    val NUM_VERTICES = adjacency.count()
    val INITIAL_RANK = 1.0 / NUM_VERTICES
    val RANDOM_JUMP = (1 - DAMPENING_FACTOR) / NUM_VERTICES
    val THRESHOLD = 0.0001 / NUM_VERTICES

    val preInitialRanks: DataSet[Page] = adjacency.flatMap {
      (adj, out: Collector[Page]) => {
        val targets = adj.neighbors
        val rankPerTarget = INITIAL_RANK * DAMPENING_FACTOR / targets.length

        // dampen fraction to targets
        for (target <- targets) {
          out.collect(Page(target, 0, rankPerTarget))
        }

        // random jump to self
        out.collect(Page(adj.url, adj.crawlDate, RANDOM_JUMP))
      }
    }
      .groupBy("url").sum("rank").andMax("crawlDate")

    val preInitialRanks2 = preInitialRanks.leftOuterJoin(tops).where(x => x.url).equalTo(x => x.url).apply((l: Page, r: Page, collector: Collector[Page]) => {
      if (r == null)
        collector.collect(l)
      else
        collector.collect(l.copy(rank = 1))
    })

    val norm: Double = Math.sqrt(preInitialRanks2.map(x => Tuple1[Double](x.rank * x.rank)).sum(0).collect().head._1)
    val initialRanks = preInitialRanks2.map(x => x.copy(rank = x.rank / norm))

    val initialDeltas = initialRanks.map { (page) =>
      Page(page.url, page.crawlDate, page.rank - INITIAL_RANK)
    }
      .withForwardedFields("url")

    val iteration = initialRanks.iterateDelta(initialDeltas, maxIterations, Array(0)) {

      (solutionSet, workset) => {
        val deltas = workset.join(adjacency).where(0).equalTo(0) {
          (lastDeltas, adj, out: Collector[Page]) => {
            val targets = adj.neighbors
            val deltaPerTarget = DAMPENING_FACTOR * lastDeltas.rank / targets.length

            for (target <- targets) {
              out.collect(Page(target, 0, deltaPerTarget))
            }
          }
        }
          .groupBy("url").sum("rank")
          .filter(x => Math.abs(x.rank) > THRESHOLD)

        val rankUpdates = solutionSet.join(deltas).where(0).equalTo(0) {
          (current, delta) => Page(current.url, current.crawlDate, current.rank + delta.rank)
        }.withForwardedFieldsFirst("url")

        (rankUpdates, deltas)
      }
    }

    iteration
  }

  def getAdjacencyDS(params: ParameterTool): DataSet[Adjacency] = {
    val readAdjacencyListsJob = Job.getInstance()
    val fs: FileSystem = FileSystem.get(readAdjacencyListsJob.getConfiguration)
    val segmentIn = new Path(params.getRequired("dir"))
    val segments = HadoopFSUtil.getPaths(fs.listStatus(segmentIn, HadoopFSUtil.getPassDirectoriesFilter(fs)))
    for (dir <- segments) {
      val adjacencyPath: Path = new Path(dir, "parse_adjacency")
      if (fs.exists(adjacencyPath)) {
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(readAdjacencyListsJob, adjacencyPath)
      }
    }
    // parameters.setBoolean("recursive.file.enumeration", true)
    val input = batch.createInput(new HadoopInputFormat[Text, Text](new SequenceFileInputFormat[Text, Text], classOf[Text], classOf[Text], readAdjacencyListsJob))
    input.map(x => JsonUtil.fromJson[Adjacency](x._2.toString))
  }

  def getTopPageDS(): DataSet[Page] = {
    val stream = StreamExecutionEnvironment.getExecutionEnvironment
    val topScore: DataStream[Page] = stream.addSource(new EsSearch("путин")).startNewChain
    val format = new TypeSerializerOutputFormat[Page]
    format.setOutputFilePath(new org.apache.flink.core.fs.Path(ES_TOP_SCORE_PATH))
    format.setWriteMode(WriteMode.OVERWRITE)
    topScore.writeUsingOutputFormat(format)
    stream.execute()
    batch.readFile(new TypeSerializerInputFormat[Page](pageTI), ES_TOP_SCORE_PATH)
  }

  def topN(ds: DataSet[Page], n: Long): DataSet[Page] = {
    ds.mapPartition((pages: Iterator[Page], collector: Collector[List[Page]]) => {
      {
        val top = mutable.PriorityQueue[Page]()(Ordering.by((v: Page) => v.rank).reverse)
        for (p <- pages) {
          top.enqueue(p)
          while (top.size > n) top.dequeue()
        }
        collector.collect(top.dequeueAll)
      }
    }).reduceGroup((pages: Iterator[List[Page]], collector: Collector[Page]) => {
      val top = mutable.PriorityQueue[Page]()(Ordering.by((v: Page) => v.rank).reverse)
      for (p <- pages) {
        p.foreach(top.enqueue(_))
        while (top.size > n) top.dequeue()
      }
      top.dequeueAll.foreach(collector.collect)
    })
  }
}
