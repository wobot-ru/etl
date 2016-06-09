package ru.wobot.etl.flink.nutch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.nutch.util.HadoopFSUtil

object Nutch {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val params = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    //properties.setProperty("bootstrap.servers", "localhost:9092")

    //    stream.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    //    stream.enableCheckpointing(5000)

    val extractor = new Extractor(ExecutionEnvironment.getExecutionEnvironment)
    val publisher = new Publisher(StreamExecutionEnvironment.getExecutionEnvironment, properties)

    def addSegment(segmentPath: Path): Unit = {
      val paths = extractor.addSegment(segmentPath)
      paths.posts match {
        case Some(p) => publisher.publishPosts(p)
      }
      paths.profiles match {
        case Some(p) => publisher.publishProfiles(p)
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
      extractor.execute()
      publisher.execute()
    }
    finally {
      val elapsedTime = System.currentTimeMillis() - startTime
      println("Export finish, elapsedTime=" + elapsedTime)
    }
  }
}
