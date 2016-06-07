package ru.wobot.etl.flink.nutch

import java.util.Properties

import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import ru.wobot.etl._


class Publisher(stream: StreamExecutionEnvironment, properties: Properties) {

  stream.getConfig.enableForceKryo()
  stream.getConfig.disableSysoutLogging

  def publishProfiles(profiles: String) = {
    stream
      .readFile(new TypeSerializerInputFormat[Profile](profileTI), profiles)
      .addSink(new FlinkKafkaProducer09[Profile]("profiles", new TypeInformationSerializationSchema[Profile](profileTI, stream.getConfig), properties))
  }

  def publishPosts(posts: String) = {
    stream
      .readFile(new TypeSerializerInputFormat[Post](postTI), posts)
      .addSink(new FlinkKafkaProducer09[Post]("posts", new TypeInformationSerializationSchema[Post](postTI, stream.getConfig), properties))
  }

  def execute() = {
    stream.execute("Publish to kafka...")
  }
}
