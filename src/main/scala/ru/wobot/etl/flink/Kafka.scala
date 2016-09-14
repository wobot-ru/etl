package ru.wobot.etl.flink

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.slf4j.{Logger, LoggerFactory}
import ru.wobot.etl._
import ru.wobot.etl.dto.DetailedPostDto

object Kafka {
  private val LOGGER: Logger = LoggerFactory.getLogger(Kafka.getClass.getName)

  val stream = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {
    println("Running read from kafka...")
    stream.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val params = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    stream.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    stream.enableCheckpointing(30000)
    stream.getConfig.setGlobalJobParameters(params)

    val profiles = stream.addSource(new FlinkKafkaConsumer09[Profile](params.getRequired(Params.TOPIC_PROFILE), new TypeInformationSerializationSchema[Profile](profileTI, stream.getConfig), properties))
    val map: DataStream[Profile] = profiles.map(x => {
      LOGGER.info(x.toString)
      x
    })
    val posts = stream.addSource(new FlinkKafkaConsumer09[Post](params.getRequired(Params.TOPIC_POST), new TypeInformationSerializationSchema[Post](postTI, stream.getConfig), properties))
    val detailedPosts = stream.addSource(new FlinkKafkaConsumer09[DetailedPost](params.getRequired(Params.TOPIC_DETAILED_POST), new TypeInformationSerializationSchema[DetailedPost](detailedPostTI, stream.getConfig), properties))

    posts.writeUsingOutputFormat(WbOutputFormat postsToProcess()).name("write posts from kafka to hbase")
    detailedPosts.writeUsingOutputFormat(WbOutputFormat directPostsToES()).name("write merged posts from kafka to hbase")
    profiles.writeUsingOutputFormat(WbOutputFormat profilesToProcess()).name("write profiles from kafka to hbase")
    LOGGER.info("Start export")
    stream.execute("Streaming data from kafka to hbase")
  }

}
