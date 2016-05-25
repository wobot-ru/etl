package ru.wobot.etl

import java.lang.Iterable

import org.apache.flink.api.common.functions.{FlatMapFunction, GroupReduceFunction}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.nutch.crawl.{CrawlDatum, Inlinks, NutchWritable}
import org.apache.nutch.metadata.{Metadata, Nutch}
import org.apache.nutch.parse.{ParseData, ParseText}
import org.apache.nutch.protocol.Content
import org.apache.nutch.segment.SegmentChecker
import org.apache.nutch.util.HadoopFSUtil

import scala.collection.JavaConverters._

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

/**
  * Implements the "WordCount" program that computes a simple word occurrence histogram
  * over some sample data
  *
  * This example shows how to:
  *
  * - write a simple Flink program.
  * - use Tuple data types.
  * - write and use user-defined functions.
  */
object SegmentExporter {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val job = org.apache.hadoop.mapreduce.Job.getInstance()
    val format = new HadoopInputFormat[Text, Writable](new SequenceFileInputFormat[Text, Writable], classOf[Text], classOf[Writable], job)
    val input = env.createInput(format)

    val dirPath = "C:\\crawl\\segments"
    val dir: Path = new Path(dirPath)
    val fs = dir.getFileSystem(job.getConfiguration);
    val fstats = fs.listStatus(dir, HadoopFSUtil.getPassDirectoriesFilter(fs));
    val files = HadoopFSUtil.getPaths(fstats);
    for (p <- files)
      if (SegmentChecker.isIndexable(p, fs)) {
        println("Add " + p)
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(p, CrawlDatum.FETCH_DIR_NAME))
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(p, CrawlDatum.PARSE_DIR_NAME))
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(p, ParseData.DIR_NAME))
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(p, ParseText.DIR_NAME))
      }

    val map = input.flatMap(new FlatMapFunction[(Text, Writable), (Text, NutchWritable)] {
      override def flatMap(value: (Text, Writable), out: Collector[(Text, NutchWritable)]): Unit =
        value._2 match {
          case datum: CrawlDatum =>
            if (!(datum.getStatus() == CrawlDatum.STATUS_LINKED || datum.getStatus() == CrawlDatum.STATUS_SIGNATURE || datum.getStatus() == CrawlDatum.STATUS_PARSE_META))
              out.collect(new Tuple2[Text, NutchWritable](value._1, new NutchWritable(value._2)))
          case c: Content => ()
          case i: Inlinks => ()
          case _ => out.collect(new Tuple2[Text, NutchWritable](value._1, new NutchWritable(value._2)))
        }
    })

    val groupBy = map.groupBy(new KeySelector[(Text, NutchWritable), Text] {
      override def getKey(value: (Text, NutchWritable)): Text = value._1
    })

    val result = groupBy.reduceGroup(new GroupReduceFunction[(Text, NutchWritable), (String, String, Post, Profile)] {
      override def reduce(values: Iterable[(Text, NutchWritable)], out: Collector[(String, String, Post, Profile)]): Unit = {
        var fetchDatum: CrawlDatum = null
        var parseText: ParseText = null
        var parseData: ParseData = null

        for (elem <- values.asScala) {
          val v = elem._2.get()
          v match {
            case c: CrawlDatum => fetchDatum = c
            case p: ParseData => parseData = p
            case p: ParseText => parseText = p
            case _ => ()
          }
        }

        if (parseData == null) return
        if (!parseData.getStatus.isSuccess || fetchDatum.getStatus != CrawlDatum.STATUS_FETCH_SUCCESS) return
        val contentMeta = parseData.getContentMeta;
        val skipFromElastic: String = contentMeta.get("nutch.content.index.elastic.skip")
        if (skipFromElastic != null && skipFromElastic.equals("1")) return
        val crawlDate: String = contentMeta.get("nutch.content.fetch.time")
        val segment = contentMeta.get(Nutch.SEGMENT_NAME_KEY);
        val isSingleDoc: Boolean = !"true".equals(contentMeta.get("nutch.parse.result.is_multiple"))
        if (isSingleDoc) {
          val parseMeta: Metadata = parseData.getParseMeta

        }

        println("segment=" + segment)
      }
    })

    //val count = groupBy.count();
    println("Total=" + result.count())
  }
}
