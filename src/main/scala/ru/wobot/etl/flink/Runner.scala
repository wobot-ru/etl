package ru.wobot.etl.flink

import org.apache.flink.api.java.utils.ParameterTool
import ru.wobot.etl.flink.nutch.Nutch

object Runner extends App {
  val params = ParameterTool.fromArgs(args)

  if (params.has("kafka")) {
    println("Run kafka")
    Kafka.main(args)
  }
  else {
    if (params.has("nutch"))
      Nutch.main(args)

    if (params.has("hbase"))
      HBase.main(args)
  }
}
