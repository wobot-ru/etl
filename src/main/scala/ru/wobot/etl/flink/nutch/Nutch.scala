package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
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
    stream.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 15000))
    stream.enableCheckpointing(3000)
    val publisher =
      if (params.has("nutch-publish")) new Publisher(stream, properties, params.getRequired("topic-post"), params.getRequired("topic-profile"))
      else null

    var segmenttoAdd = 0
    var batchSize = params.getInt("batch-size", 5)
    try {
      if (params.has("dir")) {
        val segmentIn = new Path(params.getRequired("dir"))
        val fs = segmentIn.getFileSystem(new JobConf())
        val segments = HadoopFSUtil.getPaths(fs.listStatus(segmentIn, HadoopFSUtil.getPassDirectoriesFilter(fs)))
        for (dir <- segments)
          addSegment(dir)
      }
      else
        addSegment(new Path(params.getRequired("seg")))


      if (segmenttoAdd > 0) {
        if (extractor != null) extractor.execute("Latest segments")
        if (publisher != null) publisher.execute("Latest segments")
      }
    }
    finally {
      val elapsedTime = System.currentTimeMillis() - startTime
      LOGGER.info("Export finish, elapsedTime=" + elapsedTime)
    }

    def addSegment(segmentPath: Path): Unit = {
      segmenttoAdd += 1
      println(s"Extract: $segmentPath")
      val postPath = new Path(segmentPath, "parse-posts").toString
      val profilePath = new Path(segmentPath, "parse-profiles").toString
      val kafkaExport = new Path(segmentPath, "kafka-export").toString

      if (extractor != null) {
        extractor.addSegment(segmentPath, postPath, profilePath)
        if (segmenttoAdd == batchSize) {
          extractor.execute(s"${segmentPath}")
        }
      }

      if (publisher != null) {
        LOGGER.info(s"Publish segment to kafka: $segmentPath")
        publisher.publishPosts(postPath)
        publisher.publishProfiles(profilePath)
        if (segmenttoAdd == batchSize) {
          publisher.execute(s"${segmentPath}")

        }
        //        val elements: DataStream[String] = stream.fromElements("ok")
        //        elements.writeAsText(kafkaExport)
      }

      if (segmenttoAdd == batchSize) {
        segmenttoAdd = 0
      }
    }
  }
}
