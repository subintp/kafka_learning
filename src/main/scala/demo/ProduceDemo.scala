package demo

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object ProduceDemo extends App {

  val logger = LoggerFactory.getLogger("ProductName")

  val kafkaProperties = new Properties()
  kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  kafkaProperties.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    classOf[StringSerializer]
  )
  kafkaProperties.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    classOf[StringSerializer]
  )

  val producer: KafkaProducer[String, String] =
    new KafkaProducer[String, String](kafkaProperties)

  implicit def intWithTimes(n: Int) = new {
    def times(f: => Unit) = 1 to n foreach { _ =>
      f
    }
  }

  10 times {
    val record = new ProducerRecord[String, String]("first_topic", "helloword")
    producer.send(
      record,
      new Callback {
        override def onCompletion(metadata: RecordMetadata,
                                  exception: Exception): Unit = {
          if (exception == null) {
            logger.info(s"Record sent = partition => ${metadata.partition()}")
          } else {
            logger.error("Error", exception)
          }
        }
      }
    )
  }
  producer.flush()
  producer.close()
}
