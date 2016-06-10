package ru.wobot.etl.flink.nutch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.nutch.util.HadoopFSUtil

object Nutch {
  def main(args: Array[String]): Unit = {
    println("Run nutch")
    val startTime = System.currentTimeMillis()
    val params = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    //properties.setProperty("bootstrap.servers", "localhost:9092")

    //    stream.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    //    stream.enableCheckpointing(5000)

    val extractor =
      if (params.has("nutch-extract")) new Extractor(ExecutionEnvironment.getExecutionEnvironment)
      else null

    val publisher =
      if (params.has("nutch-publish")) new Publisher(StreamExecutionEnvironment.getExecutionEnvironment, properties)
      else null

    def addSegment(segmentPath: Path): Unit = {
      val postPath = new Path(segmentPath, "parse-posts").toString
      val profilePath = new Path(segmentPath, "parse-profiles").toString

      if (extractor != null)
        extractor.addSegment(segmentPath, postPath, profilePath)

      if (publisher != null) {
        publisher.publishPosts(postPath)
        publisher.publishProfiles(profilePath)
      }
    }

    if (params.has("dir")) {
      val segmentIn = new Path(params.getRequired("dir"))
      val fs = segmentIn.getFileSystem(new JobConf())
      val segments = HadoopFSUtil.getPaths(fs.listStatus(segmentIn, HadoopFSUtil.getPassDirectoriesFilter(fs)))
      for (dir <- segments) {
        addSegment(dir)
      }
    }
    else
      addSegment(new Path(params.getRequired("seg")))

    try {
      if (extractor != null)
        extractor.execute()
      if (publisher != null)
        publisher.execute()
    }
    finally {
      val elapsedTime = System.currentTimeMillis() - startTime
      println("Export finish, elapsedTime=" + elapsedTime)
    }
  }
}
