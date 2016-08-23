package ru.wobot.etl
package flink.nutch.link

import org.apache.flink.api.java.aggregation.Aggregations

object DeltaPageRank {

  import org.apache.flink.api.scala._
  import org.apache.flink.util.Collector

  private final val DAMPENING_FACTOR: Double = 0.85
  private final val NUM_VERTICES = 5
  private final val INITIAL_RANK = 1.0 / NUM_VERTICES
  private final val RANDOM_JUMP = (1 - DAMPENING_FACTOR) / NUM_VERTICES
  private final val THRESHOLD = 0.0001 / NUM_VERTICES

  def main(args: Array[String]) {

    val maxIterations = 100

    val env = ExecutionEnvironment.getExecutionEnvironment

    val rawLines: DataSet[String] = env.fromElements(
      "1 2 3 4",
      "2 1",
      "3 5",
      "4 2 3",
      "5 2 4")
    val adjacency: DataSet[Adjacency] = rawLines
      .map(str => {
        val elements = str.split(' ')
        val id = elements(0)
        val neighbors = elements.slice(1, elements.length)
        Adjacency(id, 0, neighbors)
      })

    val out: DataSet[Page] = adjacency.flatMap {
      (adj, out: Collector[Page]) => {
        val targets = adj.neighbors
        val rankPerTarget = INITIAL_RANK * DAMPENING_FACTOR / targets.length

        // dampen fraction to targets
        for (target <- targets) {
          out.collect(Page(target, 0, rankPerTarget))
        }

        // random jump to self
        out.collect(Page(adj.url, 0, RANDOM_JUMP))
      }
    }
    out.print()

    val pre_initialRanks: DataSet[Page] = out
      .groupBy("url").sum("rank")

    val pre_initialRanks2 = pre_initialRanks.map(x => if (x.url == "3") x.copy(rank = 1) else x.copy(rank = 0))
    val sum = pre_initialRanks2.map(x => Tuple1[Double](x.rank * x.rank)).sum(0)
    val norm = Math.sqrt(sum.collect().head._1)

    val initialRanks = pre_initialRanks2.map(x => x.copy(rank = x.rank / norm))
    initialRanks.print()

    val initialDeltas = initialRanks
//    val initialDeltas = initialRanks.map { (page) => Page(page.url, 0, page.rank - INITIAL_RANK) }
//      .withForwardedFields("url")

    initialDeltas.print()

    val iteration = initialRanks.iterateDelta(initialDeltas, maxIterations, Array(0)) {

      (solutionSet, workset) => {
        val deltas = workset.join(adjacency).where(0).equalTo(0) {
          (lastDeltas, adj, out: Collector[Page]) => {
            val targets = adj.neighbors
            val deltaPerTarget = DAMPENING_FACTOR * lastDeltas.rank / targets.length

            for (target <- targets) {
              out.collect(Page(target, 0, deltaPerTarget))
            }
          }
        }
          .groupBy("url").sum("rank")
          .filter(x => Math.abs(x.rank) > THRESHOLD)

        val rankUpdates = solutionSet.join(deltas).where(0).equalTo(0) {
          (current, delta) => Page(current.url, 0, current.rank + delta.rank)
        }.withForwardedFieldsFirst("url")

        (rankUpdates, deltas)
      }
    }

    iteration.collect.sortBy(x => x.rank).reverse.foreach(println _)

  }
}
