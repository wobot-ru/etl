package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import ru.wobot.etl._
import ru.wobot.etl.dto.{PostDto, ProfileDto}

object Kafka {
  val stream = StreamExecutionEnvironment.getExecutionEnvironment


  def main(args: Array[String]): Unit = {
    stream.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val params = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    properties.setProperty("bootstrap.servers", "localhost:9092")
    //properties.setProperty("auto.offset.reset", "earliest")


    val profiles = stream.addSource(new FlinkKafkaConsumer09[Profile]("profiles", new TypeInformationSerializationSchema[Profile](profileTI, stream.getConfig), properties))
    val posts = stream.addSource(new FlinkKafkaConsumer09[Post]("posts", new TypeInformationSerializationSchema[Post](postTI, stream.getConfig), properties))
    posts
      .join(profiles)
      .where(new KeySelectorWithType[Post, String](r => r.post.profileId, TypeInformation.of(classOf[String])))
      .equalTo(new KeySelectorWithType[Profile, String](r => r.url, TypeInformation.of(classOf[String])))
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new JoinFunction[Post, Profile, (PostDto, ProfileDto)] {
        override def join(first: Post, second: Profile): (PostDto, ProfileDto) = (first.post, second.profile)
      }).print()

    //profiles.print()
    //posts.print()
    stream.execute()
  }
}
