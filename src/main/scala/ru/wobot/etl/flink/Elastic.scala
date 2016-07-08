package ru.wobot.etl.flink

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.slf4j.{Logger, LoggerFactory}
import ru.wobot.etl.DetailedPost

object Elastic {
  private val LOGGER: Logger = LoggerFactory.getLogger(HBase.getClass.getName)

  def main(args: Array[String]): Unit = {
    LOGGER.info("Run hbase")
    val params = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 120000))
    env.getConfig.enableForceKryo()

    val input = env.createInput(InputFormat.postToEs())
    val take: Seq[DetailedPost] = input.collect().take(10)
    println(take)

  }
}
