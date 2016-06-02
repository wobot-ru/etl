package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object BatchSerJava {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231730\\parse-profiles"
  val tmpPath = "file:////C:\\tmp\\flink\\seq"

  def main(args: Array[String]): Unit = {
    val batch = ExecutionEnvironment.getExecutionEnvironment
    //batch.getConfig.enableForceKryo()

    val set: DataSet[tuple.Tuple3[String, Long, String]] = batch.fromElements(new tuple.Tuple3[String, Long, String]("id1", 123l, "vk://id1"), new tuple.Tuple3[String, Long, String]("id2", 124l, "vk://id2"))
    set.write(new TypeSerializerOutputFormat[tuple.Tuple3[String, Long, String]], tmpPath, WriteMode.OVERWRITE)
    batch.execute("Writing")

    val ti = new TupleTypeInfo[tuple.Tuple3[String, Long, String]](
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    val format: TypeSerializerInputFormat[Tuple3[String, Long, String]] = new TypeSerializerInputFormat[Tuple3[String, Long, String]](ti)
    format.setFilePath(tmpPath)
    val file: DataSet[tuple.Tuple3[String, Long, String]] = batch.readFile(format, tmpPath)
    batch.createInput(format)
    file.print()
  }
}
