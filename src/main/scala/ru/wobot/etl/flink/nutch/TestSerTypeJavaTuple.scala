package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import ru.wobot.etl.Profile

object TestSerTypeJavaTuple {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231730\\parse-profiles"
  val tmpPath = "file:////C:\\tmp\\flink\\seq-java-tuple"

  def main(args: Array[String]): Unit = {
    val batch = ExecutionEnvironment.getExecutionEnvironment
    //batch.getConfig.enableForceKryo()

    val set: DataSet[tuple.Tuple3[String, Long, Profile]] = batch.fromElements(
      new tuple.Tuple3[String, Long, Profile]("id1", 123l, Profiles.p1),
      new tuple.Tuple3[String, Long, Profile]("id2", 124l, Profiles.p2),
      new tuple.Tuple3[String, Long, Profile]("id3", 1245l, Profiles.p3)
    )
    val format: TypeSerializerOutputFormat[Tuple3[String, Long, Profile]] = new TypeSerializerOutputFormat[Tuple3[String, Long, Profile]]
    set.write(format, tmpPath, WriteMode.OVERWRITE)
    set.print()
    System.in.read()
    //batch.execute("Writing")

    val ti = new TupleTypeInfo[tuple.Tuple3[String, Long, Profile]](
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.createTypeInfo(classOf[Profile]))
    val file: DataSet[tuple.Tuple3[String, Long, Profile]] = batch.readFile(new TypeSerializerInputFormat[tuple.Tuple3[String, Long, Profile]](ti), tmpPath)
    file.print()
  }
}
