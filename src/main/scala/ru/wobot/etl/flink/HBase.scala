package ru.wobot.etl.flink

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import ru.wobot.etl.dto.DetailedPostDto
import ru.wobot.etl.flink.HBaseConstants.Tables
import ru.wobot.etl.flink.OutputFormat.HBaseOutputFormat
import ru.wobot.etl.{DetailedOrWithoutAuthorPost, Post, Profile}

object HBase {
  def main(args: Array[String]): Unit = {
    println("Run hbase")
    val env = ExecutionEnvironment.getExecutionEnvironment
    val profilesToProcess = env.createInput(InputFormat.profileToProcess)
    val postsToProcess = env.createInput(InputFormat.postToProcess)
    val profiles = env.createInput(InputFormat.profilesStore)


    val admin = new HBaseAdmin(HBaseConfiguration.create)

    def disableTable(name: TableName) = {
      if (admin.isTableEnabled(name))
        admin.disableTable(name)
    }

    def truncateTable(name: TableName): Unit = {
      disableTable(name)
      admin.truncateTable(name, true)
    }

    updateProfileView(env, profilesToProcess, profiles, OutputFormat.profilesStore)
    updatePostView(env, postsToProcess, profiles)


    def updateProfileView(env: ExecutionEnvironment, processing: DataSet[Profile], saved: DataSet[Profile], profilesStore: HBaseOutputFormat[Profile]) = {
      val latest = processing.groupBy(x => x.url).sortGroup(x => x.crawlDate, Order.DESCENDING).first(1)
      val toUpdate = latest.leftOuterJoin(saved).where(0).equalTo(0).apply((l: Profile, r: Profile, collector: Collector[Profile]) => {
        if (r == null || r.crawlDate < l.crawlDate)
          collector.collect(l)
      })

      toUpdate.output(OutputFormat profilesStore)
      env.execute("Update profiles")

      truncateTable(HBaseConstants.Tables.PROFILE_TO_PROCESS)
    }

    def updatePostView(env: ExecutionEnvironment, processing: DataSet[Post], profiles: DataSet[Profile]) = {
      val latest = processing.groupBy(x => x.url).sortGroup(x => x.crawlDate, Order.DESCENDING).first(1)
      val joined = latest
        .leftOuterJoin(profiles)
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
              source = p.href,
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
      val unAuthorized = joined
        .filter(p => p.withoutAuthor.isDefined)
        .map(x => x.withoutAuthor.get)

      val autorized = joined
        .filter(p => p.withoutAuthor.isEmpty)
        .map(x => x.detailed.get)

      autorized.output(OutputFormat.postsToES)
      unAuthorized.output(OutputFormat.postsWithoutProfile)
      env.execute("Update posts")

      truncateTable(Tables.POST_TO_PROCESS)
      env.createInput(InputFormat.postWithoutProfile)
        .output(OutputFormat.postsToProces())

      env.execute("Restore un-joined posts")

      truncateTable(Tables.POST_WITHOUT_PROFILE)
    }
  }

}
