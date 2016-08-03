package ru.wobot.etl.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import ru.wobot.etl.{JsonUtil, Profile}

class ProfileKryoSerializer extends Serializer[Profile] {
  override def write(kryo: Kryo, output: Output, profile: Profile): Unit = {
    output.writeString(profile.url)
    output.writeLong(profile.crawlDate)
    output.writeString(profile.profile.toJson())

  }

  override def read(kryo: Kryo, input: Input, clazz: Class[Profile]): Profile = {
    Profile(input.readString(), input.readLong(), JsonUtil.fromJson(input.readString()))
  }
}




