package ru.wobot.etl.flink

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.io.TypeSerializerOutputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.{Logger, LoggerFactory}
import ru.wobot.etl.dto.DetailedPostDto
import ru.wobot.etl.flink.OutputFormat.HBaseOutputFormat
import ru.wobot.etl.{DetailedOrWithoutAuthorPost, Post, Profile}

object HBase {
  private val LOGGER: Logger = LoggerFactory.getLogger(HBase.getClass.getName)
  private val admin = new HBaseAdmin(HBaseConfiguration.create)

  def main(args: Array[String]): Unit = {
    LOGGER.info("Run hbase")
    val params = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 120000))
    env.getConfig.enableForceKryo()
    //env.getConfig.enableForceAvro()
    //env.getConfig.disableObjectReuse()
    //env.getConfig.enableSysoutLogging()

    if (params.has("hbase-build-profile"))
      updateProfileView(env, env.createInput(InputFormat.profileToProcess), env.createInput(InputFormat.profilesStore), OutputFormat.profilesStore)
    if (params.has("hbase-build-post"))
      updatePostView(env, env.createInput(InputFormat.postToProcess))
    if (params.has("hbase-build-post-join"))
      updatePostJoinView(env, env.createInput(InputFormat.postStore()), env.createInput(InputFormat.profilesStore))
  }

  def updateProfileView(env: ExecutionEnvironment, processing: DataSet[Profile], saved: DataSet[Profile], profilesStore: HBaseOutputFormat[Profile]) = {
    println("Build Profile View")
    LOGGER.info("{updateProfileView")
    val latest = processing.name("Processing profiles").groupBy(x => x.url).sortGroup(x => x.crawlDate, Order.DESCENDING).first(1).rebalance()
    val toUpdate = latest.leftOuterJoin(saved).where(0).equalTo(0).apply((l: Profile, r: Profile, collector: Collector[Profile]) => {
      if (r == null || r.crawlDate < l.crawlDate)
        collector.collect(l)
    }).rebalance()

    toUpdate.output(OutputFormat profilesStore).name("hbase profile view")
    //LOGGER.info(s"Add profiles=${toUpdate.count()}")
    //LOGGER.info("Start trunkate profile-to-process")
    ////truncateTable(HBaseConstants.Tables.PROFILE_TO_PROCESS)
    //LOGGER.info("End trunkate profile-to-process")
    LOGGER.info("updateProfileView}")
    env.execute("Build Profile View")
  }

  def updatePostView(env: ExecutionEnvironment, processing: DataSet[Post]) = {
    println("Build Post View")
    LOGGER.info("{updatePostView")
    val latest = processing.groupBy(x => (x.url, x.crawlDate)).sortGroup(x => x.crawlDate, Order.DESCENDING).first(1).rebalance()
    //val latest = processing.groupBy(x => x.url).sortGroup(x => x.crawlDate, Order.DESCENDING).first(1)
    latest.output(OutputFormat postsStore)
    env.execute("Build Post View")
  }

  def updatePostJoinView(env: ExecutionEnvironment, posts: DataSet[Post], profiles: DataSet[Profile]) = {
    println("Build Post-Join View")
    LOGGER.info("{updatePostJoinView")
    val joined = posts
      .leftOuterJoin(profiles, JoinHint.REPARTITION_HASH_SECOND)
      .where(x => x.post.profileId)
      .equalTo(x => x.url)
      .apply((l: Post, r: Profile, collector: Collector[DetailedOrWithoutAuthorPost]) => {
        if (r == null) {
          collector.collect(DetailedOrWithoutAuthorPost(None, Some(l)))
        }
        else {
          val p = l.post
          val pr = r.profile
          val post = new DetailedPostDto(id = p.id,
            segment = p.segment,
            crawlDate = p.crawlDate,
            href = p.href,
            source = p.source,
            profileId = p.profileId,
            smPostId = p.smPostId,
            parentPostId = p.parentPostId,
            body = p.body,
            date = p.date,
            engagement = p.engagement,
            isComment = p.isComment,
            profileCity = pr.city,
            profileGender = pr.gender,
            profileHref = pr.href,
            profileName = pr.name,
            reach = pr.reach,
            smProfileId = pr.smProfileId)
          collector.collect(DetailedOrWithoutAuthorPost(Some(post), None))
        }
      })
      .rebalance()

    joined.write(new TypeSerializerOutputFormat[DetailedOrWithoutAuthorPost], "file:///C:\\tmp\\flink\\join", WriteMode.OVERWRITE)

    val unAuthorized = joined
      .filter(p => p.withoutAuthor.isDefined)
      .map(x => x.withoutAuthor.get)
      .rebalance()
      .name("post without author")

    val autorized: DataSet[DetailedPostDto] = joined
      .filter(p => p.withoutAuthor.isEmpty)
      .map(x => x.detailed.get)
      .rebalance()
      .name("post with author")

    // autorized.write(new TypeSerializerOutputFormat[DetailedPostDto], "file:///C:\\tmp\\flink\\post-to-es", WriteMode.OVERWRITE)
    // unAuthorized.write(new TypeSerializerOutputFormat[Post], "file:///C:\\tmp\\flink\\post-without-profile", WriteMode.OVERWRITE)
    autorized.output(OutputFormat.postsToES).name("post to es")
    unAuthorized.output(OutputFormat.postsWithoutProfile).name("post without author")
    LOGGER.info("Start post update executing")
    env.execute("Build PostView View")

    //truncateTable(Tables.POST_TO_PROCESS)
    //env.createInput(InputFormat.postWithoutProfile)
    //.output(OutputFormat.postsToProces()).name("posts without profiles")

    //LOGGER.info("Start \"Restore un-joined posts\" executing")
    ////env.execute("Restore un-joined posts")
    //truncateTable(Tables.POST_WITHOUT_PROFILE)
    //LOGGER.info("updatePostView}")
  }

  def truncateTable(name: TableName): Unit = {
    disableTable(name)
    LOGGER.info(s"Truncate table: ${name.getNameAsString}")
    admin.truncateTable(name, true)
  }

  def disableTable(name: TableName) = {
    if (admin.isTableEnabled(name))
      LOGGER.info(s"Disable table: ${name.getNameAsString}")
    admin.disableTable(name)
  }
}
