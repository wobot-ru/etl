package ru.wobot.etl

object TestJson {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    val profile = new Profile();
    profile.id = "id1";
    profile.name = "trololo";

    val post = new Post()
    post.id = "post-id-1"
    println("post=" + post)
    println("profile=" + profile)


    val elapsedTime = System.currentTimeMillis() - startTime
    println("\nelapsedTime=" + elapsedTime)
  }
}
