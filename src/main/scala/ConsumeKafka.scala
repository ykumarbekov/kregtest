import java.util
import java.util.Properties

import com.typesafe.scalalogging.{LazyLogging, Logger}

import scala.collection.JavaConverters._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}


class ConsumeKafka(consumer_props:Properties) extends LazyLogging {

  def runConsume(topicName:String, cnt:String):Unit = {

    consumer_props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,cnt)
    val consumer = new KafkaConsumer[String, GenericRecord](consumer_props)
    consumer.subscribe(util.Collections.singletonList(topicName))

    try {
      val records = consumer.poll(Long.MaxValue)
      for (record <- records.asScala)
        println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}\n")
    }
      catch {
        case e:Throwable => logger.error("",e)
    } finally {
      consumer.close()
    }
  }

  def runConsume(topicName:String):Unit = {

    val consumer = new KafkaConsumer[String, GenericRecord](consumer_props)
    consumer.subscribe(util.Collections.singletonList(topicName))

    try {
      while (true) {
        val records = consumer.poll(Long.MaxValue)
        for (record <- records.asScala)
          println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}\n")
      }
    }
    catch {
      case e:Throwable => logger.error("",e)
    } finally {
      consumer.close()
    }
  }

}

object ConsumeKafka {
  def apply(consumer_props:Properties) = new ConsumeKafka(consumer_props:Properties)
}
