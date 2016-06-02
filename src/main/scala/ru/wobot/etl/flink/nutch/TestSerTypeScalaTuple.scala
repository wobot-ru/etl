package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import ru.wobot.etl.Profile

object TestSerTypeScalaTuple {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231730\\parse-profiles"
  val tmpPath = "file:////C:\\tmp\\flink\\seq-scala-tuple"

  case class Wii(id: String, date: Long, p: Profile)

  def main(args: Array[String]): Unit = {
    val batch = ExecutionEnvironment.getExecutionEnvironment

    val set: DataSet[(String, Long, Profile)] = batch.fromElements(
      ("id1", 123l, Profiles.p1),
      ("id2", 124l, Profiles.p2),
      ("id3", 1245l, Profiles.p3)
    )

    set.write(new TypeSerializerOutputFormat[(String, Long, Profile)], tmpPath, WriteMode.OVERWRITE)
    set.print()


    val ti = new TupleTypeInfo[tuple.Tuple3[String, Long, Profile]](
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.createTypeInfo(classOf[Profile]))
    val file = batch.readFile(new TypeSerializerInputFormat[tuple.Tuple3[String, Long, Profile]](ti), tmpPath)
    file.print()
  }
}
