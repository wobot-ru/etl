package ru.wobot.etl.flink

import java.io.IOException

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.hadoop.conf.{Configuration => HbaseConf}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import ru.wobot.etl._

object Kafka {
  val stream = StreamExecutionEnvironment.getExecutionEnvironment


  def main(args: Array[String]): Unit = {
    stream.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val params = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    //properties.setProperty("bootstrap.servers", "localhost:9092")
    //properties.setProperty("auto.offset.reset", "earliest")


    val profiles = stream.addSource(new FlinkKafkaConsumer09[Profile]("profile", new TypeInformationSerializationSchema[Profile](profileTI, stream.getConfig), properties))
    val posts = stream.addSource(new FlinkKafkaConsumer09[Post]("post", new TypeInformationSerializationSchema[Post](postTI, stream.getConfig), properties))
    profiles.writeUsingOutputFormat(new HBaseOutputFormat())
//    posts
//      .join(profiles)
//      .where(new KeySelectorWithType[Post, String](r => r.post.profileId, TypeInformation.of(classOf[String])))
//      .equalTo(new KeySelectorWithType[Profile, String](r => r.url, TypeInformation.of(classOf[String])))
//      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
//      .apply(new JoinFunction[Post, Profile, (PostDto, ProfileDto)] {
//        override def join(first: Post, second: Profile): (PostDto, ProfileDto) = (first.post, second.profile)
//      }).print()

    //profiles.print()
    //posts.print()
    stream.execute()
  }

  class HBaseOutputFormat extends OutputFormat[Profile] {
    private var conf: HbaseConf = null
    private var table: HTable = null

    private val serialVersionUID: Long = 1L


    override def configure(parameters: Configuration): Unit = conf = HBaseConfiguration.create

    @throws[IOException]
    override def open(taskNumber: Int, numTasks: Int) {
      table = new HTable(conf, HbaseConstants.T_PROFILE_TO_ADD)
    }

    @throws[IOException]
    override def writeRecord(p: Profile) {
      val put: Put = new Put(Bytes.toBytes(s"${p.url}|${p.crawlDate}"))
      put.add(HbaseConstants.CF_ID, HbaseConstants.C_ID, Bytes.toBytes(p.url))
      put.add(HbaseConstants.CF_ID, HbaseConstants.C_CRAWL_DATE, Bytes.toBytes(p.crawlDate))
      put.add(HbaseConstants.CF_DATA, HbaseConstants.C_JSON, Bytes.toBytes(p.profile.toJson()))
      table.put(put)
    }

    @throws[IOException]
    override def close {
      table.flushCommits
      table.close
    }
  }

}
