package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.nutch.util.HadoopFSUtil
import org.slf4j.LoggerFactory

object Nutch {
  private val LOGGER = LoggerFactory.getLogger(Nutch.getClass.getName)

  def main(args: Array[String]): Unit = {
    LOGGER.info("Run nutch")
    val startTime = System.currentTimeMillis()
    val params = ParameterTool.fromArgs(args)
    val properties = params.getProperties

    val extractor =
      if (params.has("nutch-extract")) new Extractor(ExecutionEnvironment.getExecutionEnvironment)
      else null

    val stream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    stream.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    stream.enableCheckpointing(5000)
    val publisher =
      if (params.has("nutch-publish")) new Publisher(stream, properties, params.getRequired("topic-post"), params.getRequired("topic-profile"))
      else null

    try {
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
    }
    finally {
      val elapsedTime = System.currentTimeMillis() - startTime
      LOGGER.info("Export finish, elapsedTime=" + elapsedTime)
    }

    def addSegment(segmentPath: Path): Unit = {
      println(s"Extract: $segmentPath")
      val postPath = new Path(segmentPath, "parse-posts").toString
      val profilePath = new Path(segmentPath, "parse-profiles").toString

      if (extractor != null){
        extractor.addSegment(segmentPath, postPath, profilePath)
        extractor.execute()
      }

      if (publisher != null) {
        LOGGER.info(s"Publish segment to kafka: $segmentPath")
        publisher.publishPosts(postPath)
        publisher.publishProfiles(profilePath)
        publisher.execute()
      }
    }
  }
}
