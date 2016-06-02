package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import ru.wobot.etl.Profile

object BatchSerJavaProfiles {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231730\\parse-profiles"
  val tmpPath = "file:////C:\\tmp\\flink\\seq"

  def main(args: Array[String]): Unit = {
    val batch = ExecutionEnvironment.getExecutionEnvironment
    batch.getConfig.enableForceKryo()

    val p1: Profile = new Profile()
    p1.id="vk://id1"
    val p2: Profile = new Profile()
    p2.id="vk://id2"
    val p3: Profile = new Profile()
    p3.id="vk://id3"

    val set: DataSet[tuple.Tuple3[String, Long, Profile]] = batch.fromElements(
      new tuple.Tuple3[String, Long, Profile]("id1", 123l, p1),
      new tuple.Tuple3[String, Long, Profile]("id2", 124l, p2),
      new tuple.Tuple3[String, Long, Profile]("id3", 1245l, p3)
    )
    set.write(new TypeSerializerOutputFormat[tuple.Tuple3[String, Long, Profile]], tmpPath, WriteMode.OVERWRITE)
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
