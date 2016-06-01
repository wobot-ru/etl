package ru.wobot.etl.flink.nutch

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.nutch.crawl.CrawlDatum
import org.apache.nutch.parse.{ParseData, ParseText}
import org.apache.nutch.segment.SegmentChecker
import org.apache.nutch.util.HadoopFSUtil

object SegmentPublisher {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {
    env.getConfig.enableForceKryo()

    val startTime = System.currentTimeMillis()
    val params: ParameterTool = ParameterTool.fromArgs(args)

    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    //env.enableCheckpointing(50)

    // very simple data generator
    val messageStream: DataStream[String] = env.addSource(new SourceFunction[String]() {
      val serialVersionUID = 6369260445318862378L;
      var running: Boolean = true

      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        var i: Long = 0
        while (this.running) {
          {
            ctx.collect("Element - " + ({
              i += 1;
              i - 1
            }))
            Thread.sleep(500)
          }
        }
      }


      override def cancel {
        running = false
      }

    })

    val properties: Properties = params.getProperties
    //properties.setProperty("group.id", "test-consumer-group")
    properties.setProperty("bootstrap.servers", "localhost:9092");
    messageStream.addSink(new FlinkKafkaProducer09[String]("test", new SimpleStringSchema, properties))


//    if (params.has("seg"))
//      addSegment(new Path(params.getRequired("seg")))
//
//    if (params.has("dir")) {
//      val segmentIn = new Path(params.getRequired("dir"))
//      val fs = segmentIn.getFileSystem(new JobConf())
//      val segments = HadoopFSUtil.getPaths(fs.listStatus(segmentIn, HadoopFSUtil.getPassDirectoriesFilter(fs)))
//      for (dir <- segments) {
//        addSegment(dir)
//      }
//    }

    //messageStream.addSink(new FlinkKafkaProducer09[String]("truckevent", new SimpleStringSchema, properties))
    env.execute("Exporting data from segments...")
    val elapsedTime = System.currentTimeMillis() - startTime
    println("elapsedTime=" + elapsedTime)
  }

  def addSegment(segmentPath: Path): Unit = {
    val exportJob = org.apache.hadoop.mapreduce.Job.getInstance()
    val fs = segmentPath.getFileSystem(exportJob.getConfiguration);
    if (SegmentChecker.isIndexable(segmentPath, fs)) {
      println("Export segment: " + segmentPath)
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentPath, CrawlDatum.FETCH_DIR_NAME))
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentPath, CrawlDatum.PARSE_DIR_NAME))
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentPath, ParseData.DIR_NAME))
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentPath, ParseText.DIR_NAME))

    }
  }
}
