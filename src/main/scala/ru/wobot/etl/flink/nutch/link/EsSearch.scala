package ru.wobot.etl
package flink.nutch.link

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl.{search, _}
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.elasticsearch.common.settings.Settings

@SerialVersionUID(1L)
class EsSearch(val query: String) extends RichParallelSourceFunction[Page] with CheckpointedAsynchronously[Integer] {
  private var index: Int = 0
  private var isRunning: Boolean = true

  @throws[Exception]
  def run(ctx: SourceFunction.SourceContext[Page]) {
    val settings = Settings.builder().put("cluster.name", "kviz-es").build()
    val client = ElasticClient.transport(settings, "elasticsearch://192.168.1.121:9300")

    val step: Int = getRuntimeContext.getNumberOfParallelSubtasks
    val size = step * 10

    if (index == 0) index = getRuntimeContext.getIndexOfThisSubtask
    while (isRunning) {
      val r = client.execute {
        search in "wobot" -> "post" query (query) start (index * size) size size
      }.await
      if (r.isEmpty) isRunning = false
      for (hit <- r.getHits.getHits) {
        ctx.collect(Page(hit.getId, 0, hit.getScore))
      }

      this.synchronized {
        index += step
      }
    }
  }

  def cancel() {
    isRunning = false
  }

  def snapshotState(checkpointId: Long, checkpointTimestamp: Long): Integer = index

  def restoreState(state: Integer) {
    index = state
  }
}
