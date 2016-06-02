package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import ru.wobot.etl.Profile

object BatchSer {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231730\\parse-profiles"
  val tmpPath = "file:////C:\\tmp\\flink\\seq"

  def main(args: Array[String]): Unit = {
    val batch = ExecutionEnvironment.getExecutionEnvironment
    //batch.getConfig.enableForceKryo()

    //    val set: DataSet[(String, Long, String)] = batch.fromElements(("id1", 123l, "vk://id1"), ("id2", 124l, "vk://id2"))
    //    set.write(new TypeSerializerOutputFormat[(String, Long, String)], tmpPath, WriteMode.OVERWRITE)
    //    batch.execute("Writing")

    val file: DataSet[(String, Long, String)] = batch.readFile(new TypeSerializerInputFormat[(String, Long, String)](TypeInformation.of(classOf[(String, Long, String)])), tmpPath)
    file.print()
  }
}
