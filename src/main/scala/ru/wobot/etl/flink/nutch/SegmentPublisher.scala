package ru.wobot.etl.flink.nutch

import java.util.Properties

import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import ru.wobot.etl._


class SegmentPublisher(stream: StreamExecutionEnvironment, properties: Properties) {

  stream.getConfig.enableForceKryo()
  stream.getConfig.disableSysoutLogging

  def publishProfiles(profiles: String) = {
    stream
      .readFile(new TypeSerializerInputFormat[ProfileRow](profileTI), profiles)
      .addSink(new FlinkKafkaProducer09[ProfileRow]("profiles", new TypeInformationSerializationSchema[ProfileRow](profileTI, stream.getConfig), properties))
  }

  def publishPosts(posts: String) = {
    stream
      .readFile(new TypeSerializerInputFormat[PostRow](postTI), posts)
      .addSink(new FlinkKafkaProducer09[PostRow]("posts", new TypeInformationSerializationSchema[PostRow](postTI, stream.getConfig), properties))
  }
  def execute(): Unit = {
    stream.execute("Publish to kafka...")
  }
}
