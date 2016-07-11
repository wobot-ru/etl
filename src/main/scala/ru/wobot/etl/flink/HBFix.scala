package ru.wobot.etl.flink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import ru.wobot.etl.Post

object HBFix {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableForceKryo()
    val posts: DataSet[Post] = env.createInput(InputFormat.postStore())
    val fixedPost: DataSet[Post] = posts.map(x => x.copy(post = x.post.copy(href = fixHref(x.post.id))))
    fixedPost.output(OutputFormat postsStore)
    env.execute("fix-post-href")
    //    val fb="fb://110522802337587/posts/110522802337587_904704769586049" //https://www.facebook.com/110522802337587/posts/904704769586049
    //    val vk="vk://id135637359/posts/2918" //http://vk.com/wall135637359_2918
    //    val fbComment="fb://106357469967/posts/106357469967_10154870368519968/comments/10154870368519968_10154875859849968" //https://www.facebook.com/106357469967/posts/10154870368519968?comment_id=10154875859849968
    //    val vkComment="vk://id131487475/posts/1675/comments/1678" //http://vk.com/wall131487475_1675?reply=1678
    //
    //    println(fixHref(vk))
    //    println(fixHref(vk))
    //    println(fixHref(vkComment))
    //    println(fixHref(fb))
    //    println(fixHref(fbComment))
  }

  def fixHref(h: String): String = {
    if (h.startsWith("fb")) {
      if (h.contains("comments")) buildFbCommentsHref(h)
      else buildFbPostHref(h)
    }
    else if (h.startsWith("vk")) {
      if (h.contains("comments")) buildVkCommentsHref(h)
      else buildVkPostHref(h)
    }
    else throw new RuntimeException("Unknown schema!")
  }

  // fb://106357469967/posts/106357469967_10154870368519968/comments/10154870368519968_10154875859849968 => https://www.facebook.com/106357469967/posts/10154870368519968?comment_id=10154875859849968
  def buildFbCommentsHref(h: String): String = {

    h.substring(5).split('/') match {
      case Array(userId, "posts", userId_postId, "comments", postId_commentId) => {
        val postId: String = userId_postId.split('_')(1)
        val commentId: String = postId_commentId.substring(postId_commentId.lastIndexOf('_')+1)
        s"https://www.facebook.com/$userId/posts/$postId?comment_id=$commentId"
      }
    }
  }

  //fb://110522802337587/posts/110522802337587_904704769586049  => https://www.facebook.com/110522802337587/posts/904704769586049
  def buildFbPostHref(h: String): String = {
    h.substring(h.lastIndexOf('/') + 1).split('_') match {
      case Array(userId, postId) => s"https://www.facebook.com/$userId/posts/$postId"
    }
  }


  //vk://id131487475/posts/1675/comments/1678 => http://vk.com/wall131487475_1675?reply=1678
  def buildVkCommentsHref(h: String): String = {
    h.substring(7).split('/') match {
      case Array(userId, "posts", postId, "comments", commentId) => s"http://vk.com/wall${userId}_$postId?reply=$commentId"
    }
  }

  //vk://id135637359/posts/2918 => http://vk.com/wall135637359_2918
  def buildVkPostHref(h: String): String = {
    h.substring(7).split('/') match {
      case Array(userId, "posts", postId) => s"http://vk.com/wall${userId}_$postId"
    }
  }
}
