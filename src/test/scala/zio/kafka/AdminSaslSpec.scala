package zio.kafka

import zio.clock.Clock
import zio.duration._
import zio.kafka.admin.acl._
import zio.kafka.admin.resource.{ PatternType, ResourcePattern, ResourcePatternFilter, ResourceType }
import zio.kafka.embedded.Kafka
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.TestEnvironment

import java.util.concurrent.TimeoutException

object AdminSaslSpec extends DefaultRunnableSpec with KafkaRandom {

  override def kafkaPrefix: String = "adminsaslspec"

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("client sasl admin test")(
      testM("ACLs") {
        KafkaTestUtils.withSaslAdmin() { client =>
          for {
            topic <- randomTopic
            bindings =
              Set(
                AclBinding(
                  ResourcePattern(ResourceType.Topic, name = topic, patternType = PatternType.Literal),
                  AccessControlEntry(
                    principal = "User:*",
                    host = "*",
                    operation = AclOperation.Write,
                    permissionType = AclPermissionType.Allow
                  )
                )
              )
            _ <- client.createAcls(bindings)
            createdAcls <-
              client
                .describeAcls(AclBindingFilter(ResourcePatternFilter.Any, AccessControlEntryFilter.Any))
                .repeatWhile(_.isEmpty) // because the createAcls is executed async by the broker
                .timeoutFail(new TimeoutException())(100.millis)
            deletedAcls <-
              client
                .deleteAcls(Set(AclBindingFilter(ResourcePatternFilter.Any, AccessControlEntryFilter.Any)))
            remainingAcls <-
              client
                .describeAcls(AclBindingFilter(ResourcePatternFilter.Any, AccessControlEntryFilter.Any))
                .repeatWhile(_.nonEmpty) // because the deleteAcls is executed async by the broker
                .timeoutFail(new TimeoutException())(100.millis)

          } yield assert(createdAcls)(equalTo(bindings)) &&
            assert(deletedAcls)(equalTo(bindings)) &&
            assert(remainingAcls)(equalTo(Set.empty[AclBinding]))
        }
      }
    ).provideSomeLayerShared[TestEnvironment](Kafka.saslEmbedded.mapError(TestFailure.fail) ++ Clock.live) @@ sequential

}
