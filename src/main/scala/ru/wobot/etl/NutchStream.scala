package ru.wobot.etl

import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
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
    val status= fs.listStatus(dir)
    for (dir <- status) {
      println("Add:" + dir)
      //org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(jobCrawlDatum, new org.apache.hadoop.fs.Path(dir.getPath.toString, CrawlDatum.FETCH_DIR_NAME))
      val path: Path = new Path(dir.getPath, CrawlDatum.FETCH_DIR_NAME)
      //val inFormat = new TypeSerializerInputFormat(TypeExtractor.createTypeInfo((classOf[Tuple2[Text, CrawlDatum]])))
      //val inFormat = new TypeSerializerInputFormat[(Text, CrawlDatum)](TypeExtractor.createTypeInfo((classOf[(Text, CrawlDatum)])))
      val file = env.readFile(new TextInputFormat(dir.getPath), dir.getPath.toString)
      file.print()
    }


    env.execute()
    val elapsedTime = System.currentTimeMillis() - startTime
    println("elapsedTime=" + elapsedTime)
  }

}
