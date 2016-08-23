package ru.wobot.etl
package flink.nutch.link

import java.lang.Iterable
import java.util

import org.apache.flink.api.common.functions.{GroupReduceFunction, MapPartitionFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.nutch.util.HadoopFSUtil

import scala.collection.mutable
import scala.math.Ordering

object TopNQuery {
  val EOL = System.getProperty("line.separator")
  val ES_TOP_SCORE_PATH = "C:\\crawl\\focus\\es-top-score\\"
  val TOTAL_HINT_FILE_PATH = "C:\\crawl\\focus\\hints.txt"
  val INJECT_DIR_PATH = "C:\\crawl\\focus\\to-inject\\"
  val q: String = "путин"
  val TOP_N = 1000
  val batch = ExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    val q = mutable.PriorityQueue[Page]()(Ordering.by((v: Page) => v.crawlDate).reverse)
    q.enqueue(Page("99", 22, 0), Page("10", 50, 0), Page("22", 3, 0), Page("4", 8, 0))
    println(q.head)
    println("*****")
    while (q.size > 0)
      println(q.dequeue())

    val set: DataSet[Page] = getAdjacencyDS(ParameterTool.fromArgs(args)).map(x => Page(x.url, x.crawlDate, 0))
    topN(set, 15).print()
//    set.mapPartition(new MapPartitionFunction[Page, List[Page]] {
//      val top = mutable.PriorityQueue[Page]()(Ordering.by((v: Page) => v.crawlDate).reverse)
//
//      override def mapPartition(iterable: Iterable[Page], collector: Collector[List[Page]]): Unit = {
//        val iterator: util.Iterator[Page] = iterable.iterator()
//        while (iterator.hasNext) {
//          top.enqueue(iterator.next())
//          while (top.size > 10) top.dequeue()
//        }
//
//        collector.collect(top.dequeueAll)
//      }
//    }).reduceGroup(new GroupReduceFunction[List[Page], Page] {
//      val top = mutable.PriorityQueue[Page]()(Ordering.by((v: Page) => v.crawlDate).reverse)
//
//      override def reduce(iterable: Iterable[List[Page]], collector: Collector[Page]): Unit = {
//        val iterator = iterable.iterator()
//        while (iterator.hasNext) {
//          iterator.next().foreach(top.enqueue(_))
//          while (top.size > 10) top.dequeue()
//        }
//        top.dequeueAll.foreach(collector.collect)
//      }
//    }).print() //.collect().sortBy(x=>x.crawlDate).foreach(println)
  }

  def topN(ds: DataSet[Page], n: Int): DataSet[Page] ={
    ds.mapPartition(new MapPartitionFunction[Page, List[Page]] {
      val top = mutable.PriorityQueue[Page]()(Ordering.by((v: Page) => v.crawlDate).reverse)

      override def mapPartition(iterable: Iterable[Page], collector: Collector[List[Page]]): Unit = {
        val iterator: util.Iterator[Page] = iterable.iterator()
        while (iterator.hasNext) {
          top.enqueue(iterator.next())
          while (top.size > n) top.dequeue()
        }

        collector.collect(top.dequeueAll)
      }
    }).reduceGroup(new GroupReduceFunction[List[Page], Page] {
      val top = mutable.PriorityQueue[Page]()(Ordering.by((v: Page) => v.crawlDate).reverse)

      override def reduce(iterable: Iterable[List[Page]], collector: Collector[Page]): Unit = {
        val iterator = iterable.iterator()
        while (iterator.hasNext) {
          iterator.next().foreach(top.enqueue(_))
          while (top.size > n) top.dequeue()
        }
        top.dequeueAll.foreach(collector.collect)
      }
    })
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
    val input = batch.createInput(new HadoopInputFormat[Text, Text](new SequenceFileInputFormat[Text, Text], classOf[Text], classOf[Text], readAdjacencyListsJob))
    input.map(x => JsonUtil.fromJson[Adjacency](x._2.toString))
  }
}
