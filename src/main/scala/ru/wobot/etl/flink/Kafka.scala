package ru.wobot.etl.flink

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.slf4j.{Logger, LoggerFactory}
import ru.wobot.etl._

object Kafka {
  val stream = StreamExecutionEnvironment.getExecutionEnvironment
  private val LOGGER: Logger = LoggerFactory.getLogger(Kafka.getClass.getName)

  def main(args: Array[String]): Unit = {
    println("Run kafka")
    stream.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val params = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    stream.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    stream.enableCheckpointing(30000)
    stream.getConfig.setGlobalJobParameters(params)
    //properties.setProperty("bootstrap.servers", "localhost:9092")
    //properties.setProperty("auto.offset.reset", "earliest")


    val profiles = stream.addSource(new FlinkKafkaConsumer09[Profile]("profile", new TypeInformationSerializationSchema[Profile](profileTI, stream.getConfig), properties))
    val posts = stream.addSource(new FlinkKafkaConsumer09[Post]("post", new TypeInformationSerializationSchema[Post](postTI, stream.getConfig), properties))
    //
//    val urls = posts.timeWindowAll(Time.seconds(5)).apply(new AllWindowFunction[Post, String, TimeWindow] {
//      override def apply(w: TimeWindow, iterable: Iterable[Post], collector: Collector[String]): Unit = {
//        for (p <- iterable.iterator().asScala) {
//          LOGGER.info(p.post.toJson())
//          collector.collect(p.url)
//        }
//      }
//    })

    posts.writeUsingOutputFormat(OutputFormat postsToProces).name("write posts from kafka to hbase")
    profiles.writeUsingOutputFormat(OutputFormat profilesToProces).name("write profiles from kafka to hbase")
    LOGGER.info("Start export");
    stream.execute("Insert data to hbase")
  }


}
