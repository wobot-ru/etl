package ru.wobot.etl.flink.nutch.link

import java.nio.file.{Files, Paths}

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl.{search, _}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.elasticsearch.common.settings.Settings
import ru.wobot.etl.Page

import scala.io.Source

object EsQuery {
  def main(args: Array[String]): Unit = {
    val q: String = "путин"
    val settings = Settings.builder().put("cluster.name", "kviz-es").build()
    val client = ElasticClient.transport(settings, "elasticsearch://192.168.1.121:9300")
    val totalHintFile = "c:\\crawl\\hints.txt"
    val prevTotalHints =
      if (Files.exists(Paths.get(totalHintFile))) Integer.parseInt(Source.fromFile(totalHintFile).getLines.mkString)
      else 0

    val r = client.execute {
      search in "wobot" -> "post" query "путин" size 0
    }.await

    if (r.totalHits > prevTotalHints) {
      println("!!!!!!!!!!!!!!!")
      println(r.totalHits)
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val topScore: DataStream[Page] = env.addSource(new EsSearch(q)).startNewChain
      topScore.map(x => s"${x.url}\tnutch.score=${x.rank}").writeAsText("c:\\crawl\\top-score\\")
      env.execute()
      scala.tools.nsc.io.File(totalHintFile).writeAll(r.totalHits.toString)
    }

    //    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    val collection: DataSet[Int] = env.fromCollection(List(1, 2, 3))
    //    collection.print()
  }
}
