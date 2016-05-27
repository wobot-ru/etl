package ru.wobot.etl

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.{FileStatus, Path}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.nutch.crawl.CrawlDatum


object NutchStream {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val jobCrawlDatum = org.apache.hadoop.mapreduce.Job.getInstance()


    val dir = new Path(params.getRequired("segs"))
    val fs = dir.getFileSystem()
    val status: Array[FileStatus] = fs.listStatus(dir)
    for (dir <- status) {
      println("Add:" + dir.getPath)
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobCrawlDatum, new org.apache.hadoop.fs.Path(dir.getPath.toString, CrawlDatum.FETCH_DIR_NAME))
    }
    val elapsedTime = System.currentTimeMillis() - startTime
    println("elapsedTime=" + elapsedTime)
  }
}
