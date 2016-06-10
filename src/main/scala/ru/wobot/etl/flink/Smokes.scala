package ru.wobot.etl.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.nutch.crawl.{CrawlDatum, NutchWritable}
import org.apache.nutch.parse.{ParseData, ParseText}
import org.apache.nutch.segment.SegmentChecker

object Smokes {
  def main(args: Array[String]): Unit = {
    println("Run smokes")
    println("1 - ok")
    t2()
    t3(args)
    t4(args)
  }


  def t2() = {
    val WORDS = Array[String]("To be, or not to be,--that is the question:--", "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune", "Or to take arms against a sea of troubles,", "And by opposing end them?--To die,--to sleep,--", "No more; and by a sleep to say we end", "The heartache, and the thousand natural shocks", "That flesh is heir to,--'tis a consummation", "Devoutly to be wish'd. To die,--to sleep;--", "To sleep! perchance to dream:--ay, there's the rub;", "For in that sleep of death what dreams may come,", "When we have shuffled off this mortal coil,", "Must give us pause: there's the respect", "That makes calamity of so long life;", "For who would bear the whips and scorns of time,", "The oppressor's wrong, the proud man's contumely,", "The pangs of despis'd love, the law's delay,", "The insolence of office, and the spurns", "That patient merit of the unworthy takes,", "When he himself might his quietus make", "With a bare bodkin? who would these fardels bear,", "To grunt and sweat under a weary life,", "But that the dread of something after death,--", "The undiscover'd country, from whose bourn", "No traveller returns,--puzzles the will,", "And makes us rather bear those ills we have", "Than fly to others that we know not of?", "Thus conscience does make cowards of us all;", "And thus the native hue of resolution", "Is sicklied o'er with the pale cast of thought;", "And enterprises of great pith and moment,", "With this regard, their currents turn awry,", "And lose the name of action.--Soft you now!", "The fair Ophelia!--Nymph, in thy orisons", "Be all my sins remember'd.")

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromCollection(WORDS)

    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }.map {
      (_, 1)
    }
      .groupBy(0)
      .sum(1)

    println("Printing result to stdout. Use --output to specify output path.")
    counts.print()

    println("2 - ok")
  }

  def t3(args: Array[String]) = {
    val params = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    println(properties)
    println("3 - ok")
  }

  def t4(args: Array[String]) = {
    val params = ParameterTool.fromArgs(args)
    val properties = params.getProperties
    val batch = ExecutionEnvironment.getExecutionEnvironment
    batch.getConfig.enableForceKryo()

    val exportJob = org.apache.hadoop.mapreduce.Job.getInstance()
    val segmentIn = new Path(params.getRequired("seg"))
    val fs = segmentIn.getFileSystem(exportJob.getConfiguration)
    if (SegmentChecker.isIndexable(segmentIn, fs)) {
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentIn, CrawlDatum.FETCH_DIR_NAME))
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentIn, CrawlDatum.PARSE_DIR_NAME))
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentIn, ParseData.DIR_NAME))
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(exportJob, new Path(segmentIn, ParseText.DIR_NAME))

      val input = batch.createInput(new HadoopInputFormat[Text, NutchWritable](new SequenceFileInputFormat[Text, NutchWritable], classOf[Text], classOf[NutchWritable], exportJob))
      val map = input.flatMap((t: (Text, Writable), out: Collector[(Text, NutchWritable)]) => {
        t._2 match {
          case c: CrawlDatum => if (c.getStatus == CrawlDatum.STATUS_FETCH_SUCCESS) out.collect((t._1, new NutchWritable(t._2)))
          case c: ParseData => if (c.getStatus.isSuccess) out.collect((t._1, new NutchWritable(t._2)))
          case c: ParseText => out.collect((t._1, new NutchWritable(t._2)))
        }
      })

      val data = map.groupBy(0).reduceGroup((tuples: Iterator[(Text, NutchWritable)], out: Collector[Int])=>{
        for ((url, data) <- tuples) {
          out.collect(url.hashCode())
        }
      })

      val count = data.count()

      println(s"Input.count()=$count")
    }

    println("4 - ok")
  }
}
