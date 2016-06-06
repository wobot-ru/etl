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
import ru.wobot.etl.dto.{Post, Profile}

object KafkaJoiner {
  val stream = StreamExecutionEnvironment.getExecutionEnvironment


  def main(args: Array[String]): Unit = {
    stream.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val params = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    properties.setProperty("bootstrap.servers", "localhost:9092")
    //properties.setProperty("auto.offset.reset", "earliest")


    val profiles = stream.addSource(new FlinkKafkaConsumer09[ProfileRow]("profiles", new TypeInformationSerializationSchema[ProfileRow](profileTI, stream.getConfig), properties))
    val posts = stream.addSource(new FlinkKafkaConsumer09[PostRow]("posts", new TypeInformationSerializationSchema[PostRow](postTI, stream.getConfig), properties))
    posts
      .join(profiles)
      .where(new KeySelectorWithType[PostRow, String](r => r.post.profileId, TypeInformation.of(classOf[String])))
      .equalTo(new KeySelectorWithType[ProfileRow, String](r => r.url, TypeInformation.of(classOf[String])))
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new JoinFunction[PostRow, ProfileRow, (Post, Profile)] {
        override def join(first: PostRow, second: ProfileRow): (Post, Profile) = (first.post, second.profile)
      }).print()

    //profiles.print()
    //posts.print()
    stream.execute()
  }
}
