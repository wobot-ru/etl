package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import ru.wobot.etl.Profile

object StreamEx {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231730\\parse-profiles"

  def main(args: Array[String]): Unit = {
    val stream = StreamExecutionEnvironment.getExecutionEnvironment
    stream.getConfig.enableForceAvro()

    implicit val ti: TupleTypeInfo[Tuple3[String, Long, Profile]] = new TupleTypeInfo[Tuple3[String, Long, Profile]](BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.getForClass(classOf[Profile]))

    val format: TypeSerializerInputFormat[Tuple3[String, Long, Profile]] = new TypeSerializerInputFormat[Tuple3[String, Long, Profile]](ti)
    format.setFilePath(new Path(profilePath))
    //val format = new TupleCsvInputFormat[Tuple3[String, Long, Profile]](new Path(profilePath), ti)
    val profileStream = stream.readFile(format, profilePath)
    profileStream.print()
    stream.execute()
  }
}
