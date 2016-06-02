package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object BatchSerJavaPojo {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231730\\parse-profiles"
  val tmpPath = "file:////C:\\tmp\\flink\\seq"

  def main(args: Array[String]): Unit = {
    val batch = ExecutionEnvironment.getExecutionEnvironment
    batch.getConfig.enableForceKryo()

    val pojo1: Pojo = new Pojo("vk://id1", "profileid1")
    val pojo2: Pojo = new Pojo("vk://id2", "profileid2")

    val set: DataSet[tuple.Tuple3[String, Long, Pojo]] = batch.fromElements(new tuple.Tuple3[String, Long, Pojo]("id1", 123l, pojo1), new tuple.Tuple3[String, Long, Pojo]("id2", 124l, pojo2))
    set.write(new TypeSerializerOutputFormat[tuple.Tuple3[String, Long, Pojo]], tmpPath, WriteMode.OVERWRITE)
    set.print()
    System.in.read()
    //batch.execute("Writing")

    val ti = new TupleTypeInfo[tuple.Tuple3[String, Long, Pojo]](
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.createTypeInfo(classOf[Pojo]))
    val file: DataSet[tuple.Tuple3[String, Long, Pojo]] = batch.readFile(new TypeSerializerInputFormat[tuple.Tuple3[String, Long, Pojo]](ti), tmpPath)
    file.print()
  }
}
