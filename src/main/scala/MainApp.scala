import java.nio.file.{Files, Paths}
import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import util.{JdbcPostgresData, Tools}

import scala.io.Source


object MainApp extends LazyLogging {

  //private val LOG = Logger(LoggerFactory.getLogger(this.getClass))

  val brokerAddress = "xx.xx.x.xx:9092"
  val schemaRegistryURL = "http://xx.xx.x.xx:8085"
  val tools = Tools()

  val producer_props = new Properties()
  producer_props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerAddress)
  producer_props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
  producer_props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
  producer_props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,schemaRegistryURL)

  val consumer_props = new Properties()
  consumer_props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerAddress)
  consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,classOf[StringDeserializer])
  consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG,"ksregtest-consumer-group")
  consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,classOf[io.confluent.kafka.serializers.KafkaAvroDeserializer])
  consumer_props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,schemaRegistryURL)
  consumer_props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")

  val usage = """Usage: ksregtest --mode C|P [Options]
    mode C: Consuming. Options: --topic <topic name> --count <cnt>
    where count - number of fetching messages (if count = 0 - then receive all messages)
    mode P: Producing. Options: --dbserver <IP/name> --dbname <database> --dbuser <user> --dbpassword <password> --input <file>
    where file is name of file which contains list of tables
    Example of file:
          table1|field
          table2|field
          here, table1 - table name, field - column/primary key (uses for Topic key)"""

  def main(args: Array[String]): Unit = {

    if (args.nonEmpty){

      val (options, inputs) = tools.parse(args)

      options.getOrElse("mode","-1") match {

        case "P" =>
          println("Producer mode ***********")
          val inputFile = options.getOrElse("input","-1").asInstanceOf[String]
          val dbServer = options.getOrElse("dbserver","-1").asInstanceOf[String]
          val dbName = options.getOrElse("dbname","-1").asInstanceOf[String]
          val dbUser = options.getOrElse("dbuser","-1").asInstanceOf[String]
          val dbPassword = options.getOrElse("dbpassword","-1").asInstanceOf[String]
          // ********************************************
          if (
            dbServer != "-1" &&
            dbName != "-1" &&
            inputFile != "-1" &&
            dbUser != "-1" &&
            dbPassword != "-1" &&
            Files.exists(Paths.get(inputFile)) && Files.isReadable(Paths.get(inputFile))){

            val postgresURL = "jdbc:postgresql://"+dbServer+ ":5432/" + dbName
            val jdbcPostgresQuery = JdbcPostgresData(dbUser, dbPassword, postgresURL)
            val avro2Kafka = Avro2Kafka(producer_props,schemaRegistryURL)

            Source.fromFile(inputFile).getLines()
              .foreach{l =>
              if (l.split('|').length == 2) {
                val tblName = l.split('|')(0)
                val topicKey = l.split('|')(1)
                val schemaName = dbServer+"_"+dbName+"_"+tblName
                val topicName = dbServer+"_"+dbName+"_"+tblName
                val dataResult = jdbcPostgresQuery.getResult("select * from " + tblName)
                if (dataResult.nonEmpty &&
                  avro2Kafka.RunUpload(topicName,topicKey,schemaName, dataResult))
                  println(s"Table: $tblName has been uploaded. Target topic: $topicName")
                else
                  logger.warn(s"Error happened or no any messages have been received from $dbServer pls check log file")
              }
              else logger.warn(s"parsing error, for line: $l")
            }
          }
          else {
            println("Cannot find or wrong options")
            logger.warn("Cannot find or wrong options")
          }
          // ********************************************

        case "C" =>
          println("Consumer mode ***********")
          val topicName = options.getOrElse("topic","-1").asInstanceOf[String]
          val cnt = options.getOrElse("count","-1").asInstanceOf[String]
          if (topicName != "-1" && cnt != "-1") {
            val consumeKafka = ConsumeKafka(consumer_props)
            if (cnt.toLong>0)
              consumeKafka.runConsume(topicName, cnt)
            else if (cnt.toLong == 0)
              consumeKafka.runConsume(topicName)
          }
          else {
            println("Cannot find or wrong options")
          }

        case "-1" => println(usage)
        }
      }
    else {
      println("Warning. Empty command line arguments, run with --usage option")
      logger.warn("Empty command line arguments, run with --usage option")
    }

  }

}
