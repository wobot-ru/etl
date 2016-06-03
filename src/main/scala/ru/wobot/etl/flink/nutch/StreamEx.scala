package ru.wobot.etl.flink.nutch

import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import ru.wobot.etl.Profile

//import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo

object StreamEx {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231438\\parse-profiles"
  //val profilePath = "file:////C:\\tmp\\flink\\seq-scala-tuple"

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val stream = StreamExecutionEnvironment.getExecutionEnvironment
    implicit val profileTI = createTypeInformation[(String, Long, Profile)].asInstanceOf[CaseClassTypeInfo[(String, Long, Profile)]]

    val format = new TypeSerializerInputFormat[(String, Long, Profile)](profileTI)
    val profileStream = stream.readFile(format, profilePath)
    val schemaProfile: TypeInformationSerializationSchema[(String, Long, Profile)] = new TypeInformationSerializationSchema[(String, Long, Profile)](profileTI, stream.getConfig)
    profileStream.addSink(new FlinkKafkaProducer09[(String, Long, Profile)]("p1", schemaProfile, properties))
    val delivered: DataStreamSource[(String, Long, Profile)] = stream.addSource(new FlinkKafkaConsumer09[(String, Long, Profile)]("p1", schemaProfile, properties))
    delivered.print()
    stream.execute()
  }
}
