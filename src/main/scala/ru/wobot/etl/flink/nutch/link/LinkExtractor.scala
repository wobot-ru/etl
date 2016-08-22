package ru.wobot.etl
package flink.nutch.link

import com.google.gson.Gson
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat, HadoopOutputFormat}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.nutch.crawl.{CrawlDatum, NutchWritable}
import org.apache.nutch.parse.{ParseData, ParseText}
import org.apache.nutch.segment.SegmentChecker
import org.slf4j.LoggerFactory

class LinkExtractor(val batch: ExecutionEnvironment) {
  private val LOGGER = LoggerFactory.getLogger(classOf[LinkExtractor])
  batch.getConfig.enableForceKryo()

  def execute(segmentPath: String): Unit = {
    try {
      batch.execute(s"Export data from segment $segmentPath")
    }
    catch {
      case e: RuntimeException =>
        if (!"No new data sinks have been defined since the last execution. The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'."
          .eq(e.getMessage))
          throw e
    }
  }

  def addSegment(segmentPath: Path, adjacencyPath: String): Unit = {
    val exportJob = org.apache.hadoop.mapreduce.Job.getInstance()
    val fs = segmentPath.getFileSystem(exportJob.getConfiguration)
    if (SegmentChecker.isIndexable(segmentPath, fs)) {
      val fetchDir: Path = new Path(segmentPath, CrawlDatum.FETCH_DIR_NAME)
      val parseDir: Path = new Path(segmentPath, CrawlDatum.PARSE_DIR_NAME)
      val parseData: Path = new Path(segmentPath, ParseData.DIR_NAME)
      val parseText: Path = new Path(segmentPath, ParseText.DIR_NAME)

      if (fs.exists(fetchDir) && fs.exists(parseDir) && fs.exists(parseData) && fs.exists(parseText)) {
        LOGGER.info(s"Extract segment: $segmentPath")
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, fetchDir)
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, parseDir)
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, parseData)
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, parseText)

        val input = batch.createInput(new HadoopInputFormat[Text, NutchWritable](new SequenceFileInputFormat[Text, NutchWritable], classOf[Text], classOf[NutchWritable], exportJob))
        val map = input.flatMap((t: (Text, Writable), out: Collector[(Text, NutchWritable)]) => {
          t._2 match {
            case c: CrawlDatum => if (c.getStatus == CrawlDatum.STATUS_FETCH_SUCCESS) out.collect((t._1, new NutchWritable(t._2)))
            case c: ParseData => if (c.getStatus.isSuccess) out.collect((t._1, new NutchWritable(t._2)))
            case c: ParseText => out.collect((t._1, new NutchWritable(t._2)))
          }
        })

        val data = map.rebalance().groupBy(0).reduceGroup((tuples: Iterator[(Text, NutchWritable)], out: Collector[Adjacency]) => {
          val gson = new Gson()
          def fromJson[T](json: String, clazz: Class[T]): T = {
            return gson.fromJson(json, clazz)
          }

          var key: String = null
          var fetchDatum: CrawlDatum = null
          var parseData: ParseData = null
          var parseText: ParseText = null

          for ((url, data) <- tuples) {
            data.get() match {
              case c: CrawlDatum => {
                key = url.toString
                fetchDatum = c
              }
              case d: ParseData => parseData = d
              case t: ParseText => parseText = t
              case _ => ()
            }
          }
          if (parseData != null && fetchDatum != null) {
            val contentMeta = parseData.getContentMeta
            val segment = contentMeta.get(org.apache.nutch.metadata.Nutch.SEGMENT_NAME_KEY)
            val adjacency: Adjacency = Adjacency(key, fetchDatum.getFetchTime, parseData.getOutlinks.map(x => x.getToUrl))
            out.collect(adjacency)
          }
        })

        val job = org.apache.hadoop.mapreduce.Job.getInstance()
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])
        val hadoopOutput = new HadoopOutputFormat[Text, Text](new SequenceFileOutputFormat[Text, Text], job)
        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(adjacencyPath))
        data
          .map(x => Tuple2[Text, Text](new Text(x.url), new Text(JsonUtil.toJson(x))))
          .output(hadoopOutput)
        //        data.write(new TypeSerializerOutputFormat[Adjacency], adjacencyPath, WriteMode.OVERWRITE)
        //        val hadoopOutputFormat = new HadoopOutputFormat[Text, Text](new TextOutputFormat[Text, Text], new JobConf)
        //        hadoopOutputFormat.getJobConf.set("mapred.textoutputformat.separator", " ")
        //        FileOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf, new Path(adjacencyPath + ".seq"))
        //        data.map(x => Tuple2[Text, Text](new Text(x.url), new Text(JsonUtil.toJson(x)))).output(hadoopOutputFormat)
        //        data.writeAsText(adjacencyPath + ".txt", WriteMode.OVERWRITE)
      }
      else
        LOGGER.info(s"Skip segment: $segmentPath")
    }
  }
}
