package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.functions.{FilterFunction, JoinFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import ru.wobot.etl.{Post, Profile}

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
    stream.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("auto.offset.reset", "earliest")


    val profiles: DataStreamSource[(String, Long, Profile)] = stream.addSource(new FlinkKafkaConsumer09[(String, Long, Profile)]("profiles", profileSchema, properties))
    val posts = stream.addSource(new FlinkKafkaConsumer09[(String, Long, Post)]("posts", postSchema, properties))
    val filtredPost = posts.filter(new FilterFunction[(String, Long, Post)] {
      override def filter(value: (String, Long, Post)): Boolean = value._1 != null && value._3 != null && value._3.profileId != null
    })
    val filtredProfile = profiles.filter(new FilterFunction[(String, Long, Profile)] {
      override def filter(value: (String, Long, Profile)): Boolean = value._1 != null && value._3 != null
    })
    //filtredProfile.print()

    filtredPost
      .join(filtredProfile)
      .where(new KeySelectorWithType[(String, Long, Post), String]((tuple: (String, Long, Post)) => tuple._3.profileId, TypeInformation.of(classOf[String])))
      .equalTo(new KeySelectorWithType[(String, Long, Profile), String]((tuple: (String, Long, Profile)) => tuple._1, TypeInformation.of(classOf[String])))
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new JoinFunction[(String, Long, Post), (String, Long, Profile), (Post, Profile)] {
        override def join(first: (String, Long, Post), second: (String, Long, Profile)): (Post, Profile) = (first._3, second._3)
      }).print()

    //    profiles.print()
    //    posts.print()
    stream.execute()
  }
}
