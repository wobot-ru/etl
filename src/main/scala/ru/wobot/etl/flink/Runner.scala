package ru.wobot.etl.flink

import org.apache.flink.api.java.utils.ParameterTool
import ru.wobot.etl.flink.Params._
import ru.wobot.etl.flink.nutch.Nutch

object Runner {
  def main(args: Array[String]): Unit = {
    println(s"Usages: --smokes --kafka --nutch [--nutch-extract --nutch-publish] --hbase --$HBASE_EXPORT --$UPLOAD_TO_ES")

    val params = ParameterTool.fromArgs(args)
    if (params.has("smokes"))
      Smokes.main(args)

    if (params.has("kafka")) {
      Kafka.main(args)
    }
    else {
      if (params.has("nutch")) Nutch.main(args)

      if (params.has("hbase")) HBase.main(args)

      if (params.has(HBASE_EXPORT) || params.has(UPLOAD_TO_ES))
        Elastic.main(args)
    }
  }

}
