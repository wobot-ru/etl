package ru.wobot.etl.flink.nutch

import org.apache.flink.api.common.io.{BinaryInputFormat, BinaryOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import ru.wobot.etl.{JsonUtil, Profile}

object TestSerBinScalaTuple {

  val profilePath = "file:////C:\\crawl\\segments\\20160527231730\\parse-profiles"
  val tmpPath = "file:////C:\\tmp\\flink\\seq-bin"

  def main(args: Array[String]): Unit = {
    val batch = ExecutionEnvironment.getExecutionEnvironment
    //batch.getConfig.enableForceKryo()

    val set: DataSet[(String, Long, Profile)] = batch.fromElements(
      ("id1", 123l, Profiles.p1),
      ("id2", 124l, Profiles.p2),
      ("id3", 1245l, Profiles.p3)
    )

    set.write(new BinaryOutputFormat[(String, Long, Profile)] {
      override def serialize(t: (String, Long, Profile), out: DataOutputView): Unit = {
        out.writeUTF(t._1)
        out.writeLong(t._2)
        val json: String = JsonUtil.toJson(t._3)
        out.writeUTF(json)
      }
    }, tmpPath, WriteMode.OVERWRITE)
    set.print()
    //System.in.read()
//    batch.execute("Writing")

    val input = new BinaryInputFormat[(String, Long, Profile)] {
      override def deserialize(t: (String, Long, Profile), in: DataInputView): (String, Long, Profile) = {
        val id: String = in.readUTF()
        val date = in.readLong()
        val json = in.readUTF()
        val p: Profile = JsonUtil.fromJson[Profile](json)
        (id, date, p)
      }
    }

    input.setFilePath(tmpPath)
    //    val ti = new TupleTypeInfo[(String, Long, Profile)](
    //      BasicTypeInfo.STRING_TYPE_INFO,
    //      BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.createTypeInfo(classOf[Profile]))
    //    val file: DataSet[(String, Long, Profile)] = batch.readFile(format, tmpPath)
    //    val format = new TypeSerializerInputFormat[(String, Long, Profile)](TypeInformation.of(classOf[(String, Long, Profile)]))
    //    format.setFilePath(tmpPath)
    val file = batch.createInput(input)
    file.print()
  }
}
