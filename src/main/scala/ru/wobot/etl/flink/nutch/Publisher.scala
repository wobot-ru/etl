package ru.wobot.etl.flink.nutch

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import ru.wobot.etl._

import scala.collection.mutable.ListBuffer

class Publisher(stream: StreamExecutionEnvironment, properties: Properties, fs: FileSystem,
                topicPost: String,
                topicDetailedPost: String,
                topicProfile: String) {
  private val LOGGER = LoggerFactory.getLogger(classOf[Publisher])

  stream.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 15000))
  stream.enableCheckpointing(3000)

  val posts = ListBuffer[String]()
  val detailedPosts = ListBuffer[String]()
  val profiles = ListBuffer[String]()
  stream.getConfig.enableForceKryo()

  def publishProfiles(profile: String) = {
    profiles += profile
  }

  def publishPosts(post: String) = {
    posts += post
  }

  def publishDetailedPosts(detailedPost: String) = {
    detailedPosts += detailedPost
  }

  def execute(segmentPath: String) = {
    for (post <- posts) {
      if (fs.exists(new Path(post))) {
        stream
          .readFile(new TypeSerializerInputFormat[Post](postTI), post)
          .addSink(new FlinkKafkaProducer09[Post](topicPost, new TypeInformationSerializationSchema[Post](postTI, stream.getConfig), properties))
      }
      else
        LOGGER.info(s"Skip import, file not exist: $post")
    }
    for (detailedPost <- detailedPosts) {
      if (fs.exists(new Path(detailedPost))) {
        stream
          .readFile(new TypeSerializerInputFormat[DetailedPost](detailedPostTI), detailedPost)
          .addSink(new FlinkKafkaProducer09[DetailedPost](topicDetailedPost, new TypeInformationSerializationSchema[DetailedPost](detailedPostTI, stream.getConfig), properties))
      }
      else
        LOGGER.info(s"Skip import, file not exist: $detailedPost")
    }
    for (profile <- profiles) {
      if (fs.exists(new Path(profile))) {
        stream
          .readFile(new TypeSerializerInputFormat[Profile](profileTI), profile)
          .addSink(new FlinkKafkaProducer09[Profile](topicProfile, new TypeInformationSerializationSchema[Profile](profileTI, stream.getConfig), properties))
      }
      else
        LOGGER.info(s"Skip import, file not exist: $profile")
    }
    val execute = stream.execute(s"Publish to kafka $segmentPath")
  }
}
