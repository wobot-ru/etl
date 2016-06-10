package ru.wobot.etl.flink

import org.apache.flink.api.java.utils.ParameterTool
import ru.wobot.etl.flink.nutch.Nutch

object Runner {
  def main(args: Array[String]): Unit = {
    println("Usages: --smokes --kafka --nutch [--nutch-extract --nutch-publish] --hbase")

    val params = ParameterTool.fromArgs(args)

    if (params.has("smokes"))
      println("Run smokes")
      Smokes.main(args)

    if (params.has("kafka")) {
      println("Run kafka")
      Kafka.main(args)
    }
    else {
      if (params.has("nutch"))
        println("Run nutch")
        Nutch.main(args)

      if (params.has("hbase"))
        println("Run hbase")
        HBase.main(args)
    }
  }

}
