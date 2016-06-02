package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import ru.wobot.etl.Profile
import org.apache.flink.api.scala._
import org.apache.flink.core.memory.DataOutputView

object StreamExSc {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231730\\parse-profiles"

  def main(args: Array[String]): Unit = {
    val stream = StreamExecutionEnvironment.getExecutionEnvironment
    stream.getConfig.enableForceKryo()

    //implicit val ti = new TupleTypeInfo[(String, Long, Profile)](BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.getForClass(classOf[Profile]))

    val ti: TypeInformation[(String, Long, Profile)] = TypeInformation.of(classOf[(String, Long, Profile)])
    val serializer: TypeSerializer[(String, Long, Profile)] = ti.createSerializer(stream.getConfig)
    //    val profile: Profile = new Profile()
    //    profile.id="123123"
    //    serializer.serialize(("123123",324423l,profile), new )

    val format: TypeSerializerInputFormat[(String, Long, Profile)] = new TypeSerializerInputFormat[(String, Long, Profile)](ti)
    format.setFilePath(new Path(profilePath))
    //val format = new TupleCsvInputFormat[Tuple3[String, Long, Profile]](new Path(profilePath), ti)
    val profileStream = stream.createInput(format, ti)
    profileStream.print()
    stream.execute()
  }
}
