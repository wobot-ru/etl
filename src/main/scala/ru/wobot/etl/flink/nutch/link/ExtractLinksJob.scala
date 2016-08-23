package ru.wobot.etl.flink.nutch.link

package ru.wobot.etl.flink.nutch.link

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.nutch.util.HadoopFSUtil
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._

object ExtractLinksJob {
  private val LOGGER = LoggerFactory.getLogger(ExtractLinksJob.getClass.getName)

  def main(args: Array[String]): Unit = {
    LOGGER.info("Run nutch")
    val startTime = System.currentTimeMillis()
    val params = ParameterTool.fromArgs(args)

    val extractor = new LinkExtractor(ExecutionEnvironment.getExecutionEnvironment)

    val fs: FileSystem = FileSystem.get(new JobConf())
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
      }
    }
    finally {
      val elapsedTime = System.currentTimeMillis() - startTime
      LOGGER.info("Export finish, elapsedTime=" + elapsedTime)
    }

    def addSegment(segmentPath: Path): Unit = {
      segmentToAdd += 1
      println(s"Extract: $segmentPath")
      val adjacencyPath = new Path(segmentPath, "parse_adjacency").toString

      if (extractor != null) {
        extractor.addSegment(segmentPath, adjacencyPath)
        if (segmentToAdd == batchSize) {
          extractor.execute(s"${segmentPath}")
        }
      }

      if (segmentToAdd == batchSize) {
        segmentToAdd = 0
      }
    }
  }
}
