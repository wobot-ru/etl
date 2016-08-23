package ru.wobot.etl
package flink.nutch.link

object TopicPageRank {

  import org.apache.flink.api.scala._
  import org.apache.flink.util.Collector

  private final val DAMPENING_FACTOR: Double = 0.85
  private final val NUM_VERTICES = 5
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

    val initialRanks: DataSet[Page] = adjacency.flatMap {
      (adj, out: Collector[Page]) => {
        if (adj.url == "3")
          out.collect(Page(adj.url, 0, 1))
        else out.collect(Page(adj.url, 0, 0))
      }
    }

    val iteration = initialRanks.iterateDelta(initialRanks, maxIterations, Array(0)) {

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
