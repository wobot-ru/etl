package ru.wobot.etl.flink.nutch

import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import ru.wobot.etl.Profile

//import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo

object StreamEx {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231730\\parse-profiles"
  //val profilePath = "file:////C:\\tmp\\flink\\seq-scala-tuple"

  def main(args: Array[String]): Unit = {
    val stream = StreamExecutionEnvironment.getExecutionEnvironment
    //stream.getConfig.enableForceKryo()
    //
    //    val ti = new TupleTypeInfo[tuple.Tuple3[String, Long, Profile]](
    //      BasicTypeInfo.STRING_TYPE_INFO,
    //      BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.createTypeInfo(classOf[Profile]))

    implicit val information = createTypeInformation[(String, Long, Profile)].asInstanceOf[CaseClassTypeInfo[(String, Long, Profile)]]

    val format = new TypeSerializerInputFormat[(String, Long, Profile)](information)
    val profileStream = stream.readFile(format, profilePath)
    profileStream.print()
    stream.execute()
  }
}
