package demo

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object ConsumerDemo extends App {
  println("hello world")

  val logger: Logger = LoggerFactory.getLogger("ConsumerDemo")

  val kafkaProperties = new Properties()
  kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  kafkaProperties.put(
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer"
  )

  kafkaProperties.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer"
  )

  kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "my fisth application")
  kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[String, String](kafkaProperties)

  consumer.subscribe(util.Arrays.asList("first_topic"))

  while (true) {
    val record = consumer.poll(Duration.ofMillis(100)).asScala
    for (data <- record.iterator) {
      println(data.value())
      println(data.partition())
    }
  }
}
