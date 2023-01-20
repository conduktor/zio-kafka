package zio.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import zio.kafka.consumer.{ Consumer, Subscription }
import zio.kafka.embedded.Kafka
import zio.kafka.producer.Producer
import zio.kafka.serde.Serde
import zio.test.TestAspect._
import zio.test._

object OOMSpecXmx300m extends ZIOSpecWithSslKafka {

  override val kafkaPrefix: String = "oom-spec"
  override def spec: Spec[Kafka, Any] =
    suite("OOM check")(
      test("producer should fail with ssl check") {
        for {
          result <- (for {
                      topic <- randomTopic
                      _     <- Producer.produce(new ProducerRecord(topic, "boo", "baa"), Serde.string, Serde.string)
                    } yield ()).provideSomeLayer(KafkaTestUtils.producer).exit
        } yield assertTrue(result.isFailure) &&
          assertTrue(
            result.toEither.left.map(_.getMessage()) == Left(
              "Received an unexpected SSL packet from the server. Please ensure the client is properly configured with SSL enabled"
            )
          )
      },
      test("consumer should fail with ssl check") {
        for {
          result <- (for {
                      topic <- randomTopic
                      _     <- Consumer.subscribe(Subscription.Topics(Set(topic)))
                    } yield ()).provideSomeLayer(KafkaTestUtils.consumer("test")).exit
        } yield assertTrue(result.isFailure) &&
          assertTrue(
            result.toEither.left.map(_.getMessage()) == Left(
              "Received an unexpected SSL packet from the server. Please ensure the client is properly configured with SSL enabled"
            )
          )
      },
      test("admin client should fail with ssl check") {
        for {
          result <- (KafkaTestUtils.withAdmin { client =>
                      client.listTopics()
                    }).exit
        } yield assertTrue(result.isFailure) &&
          assertTrue(
            result.toEither.left.map(_.getMessage()) == Left(
              "Received an unexpected SSL packet from the server. Please ensure the client is properly configured with SSL enabled"
            )
          )
      }
    ) @@ withLiveClock @@ sequential
}
