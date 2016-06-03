package ru.wobot.etl.flink.nutch

import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import ru.wobot.etl.{Post, Profile}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo

object KafkaJoiner {
  val stream = StreamExecutionEnvironment.getExecutionEnvironment
  implicit val postTI = createTypeInformation[(String, Long, Post)].asInstanceOf[CaseClassTypeInfo[(String, Long, Post)]]
  implicit val profileTI = createTypeInformation[(String, Long, Profile)].asInstanceOf[CaseClassTypeInfo[(String, Long, Profile)]]

  val postOutFormat: TypeSerializerOutputFormat[(String, Long, Post)] = new TypeSerializerOutputFormat[(String, Long, Post)]
  val postInFormat = new TypeSerializerInputFormat[(String, Long, Post)](postTI)
  val postSchema = new TypeInformationSerializationSchema[(String, Long, Post)](postTI, stream.getConfig)

  val profileOutFormat = new TypeSerializerOutputFormat[(String, Long, Profile)]
  val profileInFormat = new TypeSerializerInputFormat[(String, Long, Profile)](profileTI)
  val profileSchema = new TypeInformationSerializationSchema[(String, Long, Profile)](profileTI, stream.getConfig)


  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    properties.setProperty("bootstrap.servers", "localhost:9092")


    val profiles = stream.addSource(new FlinkKafkaConsumer09[(String, Long, Profile)]("profiles", profileSchema, properties))
    profiles.print()
    //val posts = stream.addSource(new FlinkKafkaConsumer09[(String, Long, Post)]("posts", postSchema, properties))
    //posts.print()
    stream.execute()
  }
}
