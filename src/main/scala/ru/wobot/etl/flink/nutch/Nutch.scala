package ru.wobot.etl.flink.nutch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.nutch.util.HadoopFSUtil
import org.slf4j.LoggerFactory
import ru.wobot.etl.flink.Params

import scala.util.control.Breaks._

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

    val fs: FileSystem = FileSystem.get(new JobConf())
    val publisher =
      if (params.has("nutch-publish")) new Publisher(StreamExecutionEnvironment.getExecutionEnvironment,
        properties, fs, params.getRequired(Params.TOPIC_POST),
        params.getRequired(Params.TOPIC_DETAILED_POST),
        params.getRequired(Params.TOPIC_PROFILE)
      )
      else null

    val batchSize = params.getInt("batch-size", 1)
    val startSeg = params.get("nutch-seg-start", null)
    val stopSeg = params.get("nutch-seg-stop", null)
    var segmentToAdd = 0
    var latestSeg = "NO_SEG"

    try {
      if (params.has("dir")) {
        val segmentIn = new Path(params.getRequired("dir"))
        val segments = HadoopFSUtil.getPaths(fs.listStatus(segmentIn, HadoopFSUtil.getPassDirectoriesFilter(fs)))
        breakable {
          for (dir <- segments) {
            if (startSeg == null) {
              latestSeg = dir.toString
              addSegment(dir)
            }
            else {
              if (dir.getName >= startSeg) {
                latestSeg = dir.toString
                addSegment(dir)
              }
              else
                LOGGER.info(s"Skip segment: $dir")
            }
            if (stopSeg != null && stopSeg <= dir.getName)
              break
          }
        }
      }

      if (params.has("seg")) {
        latestSeg = params.getRequired("seg")
        val latestSegPath: Path = new Path(latestSeg)
        addSegment(latestSegPath)
      }

      if (segmentToAdd > 0) {
        if (extractor != null) extractor.execute(latestSeg)
        if (publisher != null) publisher.execute(latestSeg)
      }
    }
    finally {
      val elapsedTime = System.currentTimeMillis() - startTime
      LOGGER.info("Export finish, elapsedTime=" + elapsedTime)
    }

    def addSegment(segmentPath: Path): Unit = {
      segmentToAdd += 1
      println(s"Extract: $segmentPath")
      val postPath = new Path(segmentPath, "parse_posts").toString
      val detailedPostPath = new Path(segmentPath, "parse_detailed_posts").toString
      val profilePath = new Path(segmentPath, "parse_profiles").toString

      if (extractor != null) {
        extractor.addSegment(segmentPath, postPath, detailedPostPath, profilePath)
        if (segmentToAdd == batchSize) {
          extractor.execute(s"$segmentPath")
        }
      }

      if (publisher != null) {
        LOGGER.info(s"Publish segment to kafka: $segmentPath")
        publisher.publishPosts(postPath)
        publisher.publishDetailedPosts(detailedPostPath)
        publisher.publishProfiles(profilePath)

        if (segmentToAdd == batchSize) {
          publisher.execute(s"$segmentPath")
        }
      }

      if (segmentToAdd == batchSize) {
        segmentToAdd = 0
      }
    }
  }
}
