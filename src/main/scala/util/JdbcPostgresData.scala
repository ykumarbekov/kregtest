package util

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.typesafe.scalalogging.Logger
import org.json.{JSONArray, JSONObject}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class JdbcPostgresData(user:String, pwd:String, url:String) {

  private val LOG = Logger(LoggerFactory.getLogger(this.getClass))

  private val props = new Properties()
  props.setProperty("user", user)
  props.setProperty("password", pwd)
  private var conn:Connection = _
  private var jsonobject: JSONObject = _
  private val jsonArray = new JSONArray

  def getResult(sql:String):List[JSONObject] = {
    val resultList = ListBuffer[JSONObject]()

    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(url, props)
      val statement = conn.createStatement()
      val resultSet = statement.executeQuery(sql)

      while (resultSet.next()) {
        val metaData = resultSet.getMetaData
        var i = 0
        jsonobject = new JSONObject()

        while (i < metaData.getColumnCount) {
          if (resultSet.getObject(i + 1) != null)
            jsonobject.put(metaData.getColumnLabel(i + 1),resultSet.getObject(i + 1))
          else
            jsonobject.put(metaData.getColumnLabel(i + 1),JSONObject.NULL)
          i += 1
        }
        resultList += jsonobject
      }
    } catch {
      case e:Throwable => LOG.error("",e)
    }
    if (conn != null) conn.close()
    resultList.toList
  }
}

object JdbcPostgresData{
  def apply(user:String,pwd:String,url:String) = new JdbcPostgresData(user:String,pwd:String,url:String)
}
