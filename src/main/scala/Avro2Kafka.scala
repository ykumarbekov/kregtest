import java.util.Properties

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json.JSONObject
import org.slf4j.LoggerFactory
import tech.allegro.schema.json2avro.converter.JsonAvroConverter
import util.Tools

class Avro2Kafka(producer_props:Properties, schemaRegistryURL:String) extends LazyLogging {

  private val requestSchemaURL = schemaRegistryURL + "/subjects/"
  private val tools = Tools()

  def RunUpload(topicName:String, topicKey:String, schemaName:String, queryResult:List[JSONObject]): Boolean = {

    val schemaData = tools.getDataFromUrl(requestSchemaURL  + schemaName + "/versions/latest")
    var status = true

    if (schemaData.isDefined) {

      val schemaStr = schemaData.get.get("schema").toString
      val producer = new KafkaProducer[String, GenericRecord](producer_props)

      // Convert Data to avro
      val schema = new Schema.Parser().parse(schemaStr)
      val converter = new JsonAvroConverter()

      //for (x <- queryResult) println(x)

      for (x <- queryResult) {
        producer.send(new ProducerRecord(
          topicName,
          x.get(topicKey).toString,
          converter.convertToGenericDataRecord(x.toString.getBytes, schema)))
      }

      logger.info(s"Return result size: ${queryResult.size}.")

      producer.flush()
      producer.close()
    }
    else {
      logger.warn("Cannot fetch schema from URL: " + requestSchemaURL + schemaName + "/versions/latest")
      status = false
    }
    status
  }
}

object Avro2Kafka{
  def apply(producer_props:Properties,schemaRegistryURL:String) = new Avro2Kafka(producer_props:Properties,schemaRegistryURL:String)
}
