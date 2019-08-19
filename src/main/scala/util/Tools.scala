package util

import com.typesafe.scalalogging.{LazyLogging, Logger}
import okhttp3.{OkHttpClient, Request}
import org.json.JSONObject
import org.slf4j.LoggerFactory

class Tools extends LazyLogging {

  private val client = new OkHttpClient

  private val map: Map[String, Any] = Map()
  private val list: List[Any] = List()

  def getDataFromUrl(url:String):Option[JSONObject] = {
    var result:Option[JSONObject] = None
    try{
     val request = new Request.Builder().url(url).get().build()
     val response = client.newCall(request).execute()
     if (response.code() == 200) {
       result = Some(new JSONObject(response.body().string()))
     }
      response.close()
    }catch {
      case e:Throwable => logger.error(e.getMessage)
    }
    result
  }

  def parse(args: Array[String]): (Map[String, Any], List[Any]) = _parse(map, list, args.toList)

  private [this] def _parse(map: Map[String, Any], list: List[Any], args: List[String]): (Map[String, Any], List[Any]) = {
    args match {
      case Nil => (map, list)
      case arg :: value :: tail if arg.startsWith("--") && !value.startsWith("--") => _parse(map ++ Map(arg.substring(2) -> value), list, tail)
      case arg :: tail if arg.startsWith("--") => _parse(map ++ Map(arg.substring(2) -> true), list, tail)
      case opt :: tail => _parse(map, list :+ opt, tail)
    }
  }

}

object Tools {
  def apply() = new Tools()
}
