package ru.wobot.etl.flink.nutch

import java.util.Properties

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import ru.wobot.etl._


class Publisher(stream: StreamExecutionEnvironment, properties: Properties, topicPost: String, topicProfile: String) {

  stream.getConfig.enableForceKryo()

  def publishProfiles(profiles: String) = {
    stream
      .readFile(new TypeSerializerInputFormat[Profile](profileTI), profiles)
      .addSink(new FlinkKafkaProducer09[Profile](topicProfile, new TypeInformationSerializationSchema[Profile](profileTI, stream.getConfig), properties))
  }

  def publishPosts(posts: String) = {
    stream
      .readFile(new TypeSerializerInputFormat[Post](postTI), posts)
      .addSink(new FlinkKafkaProducer09[Post](topicPost, new TypeInformationSerializationSchema[Post](postTI, stream.getConfig), properties))
  }

  def execute(segmentPath : String) = {
    val execute = stream.execute(s"Publish to kafka $segmentPath")
  }
}
