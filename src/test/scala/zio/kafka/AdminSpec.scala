package zio.kafka.admin

import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource
import org.apache.kafka.clients.admin.{ ConfigEntry, RecordsToDelete }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.{ Node => JNode }
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.Duration
import zio.kafka.KafkaTestUtils
import zio.kafka.KafkaTestUtils._
import zio.kafka.admin.AdminClient.{
  AlterConfigOp,
  AlterConfigOpType,
  AlterConfigsOptions,
  ConfigResource,
  ConfigResourceType,
  ConsumerGroupDescription,
  ConsumerGroupState,
  KafkaConfig,
  ListConsumerGroupOffsetsOptions,
  ListConsumerGroupsOptions,
  OffsetAndMetadata,
  OffsetSpec,
  TopicPartition
}
import zio.kafka.consumer.{ Consumer, OffsetBatch, Subscription }
import zio.kafka.embedded.Kafka
import zio.kafka.serde.Serde
import zio.stream.ZTransducer
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.TestEnvironment
import zio.{ Chunk, Has, Schedule, ZIO }

import java.util.UUID

object AdminSpec extends DefaultRunnableSpec {
  override def spec =
    suite("client admin test")(
      testM("create, list, delete single topic") {
        KafkaTestUtils.withAdmin { client =>
          for {
            list1 <- client.listTopics()
            _     <- client.createTopic(AdminClient.NewTopic("topic1", 1, 1))
            list2 <- client.listTopics()
            _     <- client.deleteTopic("topic1")
            list3 <- client.listTopics()
          } yield assert(list1.size)(equalTo(0)) &&
            assert(list2.size)(equalTo(1)) &&
            assert(list3.size)(equalTo(0))

        }
      },
      testM("create, list, delete multiple topic") {
        KafkaTestUtils.withAdmin { client =>
          for {
            list1 <- client.listTopics()
            _ <- client.createTopics(List(AdminClient.NewTopic("topic2", 1, 1), AdminClient.NewTopic("topic3", 4, 1)))
            list2 <- client.listTopics()
            _     <- client.deleteTopic("topic2")
            list3 <- client.listTopics()
            _     <- client.deleteTopic("topic3")
            list4 <- client.listTopics()
          } yield assert(list1.size)(equalTo(0)) &&
            assert(list2.size)(equalTo(2)) &&
            assert(list3.size)(equalTo(1)) &&
            assert(list4.size)(equalTo(0))

        }
      },
      testM("just list") {
        KafkaTestUtils.withAdmin { client =>
          for {
            list1 <- client.listTopics()
          } yield assert(list1.size)(equalTo(0))

        }
      },
      testM("create, describe, delete multiple topic") {
        KafkaTestUtils.withAdmin { client =>
          for {
            list1 <- client.listTopics()
            _ <- client.createTopics(List(AdminClient.NewTopic("topic4", 1, 1), AdminClient.NewTopic("topic5", 4, 1)))
            descriptions <- client.describeTopics(List("topic4", "topic5"))
            _            <- client.deleteTopics(List("topic4", "topic5"))
            list3        <- client.listTopics()
          } yield assert(list1.size)(equalTo(0)) &&
            assert(descriptions.size)(equalTo(2)) &&
            assert(list3.size)(equalTo(0))

        }
      },
      testM("create, describe topic config, delete multiple topic") {
        KafkaTestUtils.withAdmin { client =>
          for {
            list1 <- client.listTopics()
            _ <- client.createTopics(List(AdminClient.NewTopic("topic6", 1, 1), AdminClient.NewTopic("topic7", 4, 1)))
            configResources = List(
                                ConfigResource(ConfigResourceType.Topic, "topic6"),
                                ConfigResource(ConfigResourceType.Topic, "topic7")
                              )
            configsAsync = client.describeConfigsAsync(configResources).flatMap { configs =>
                             ZIO.foreachPar(configs) { case (resource, configTask) =>
                               configTask.map(config => (resource, config))
                             }
                           }
            (configs, awaitedConfigsAsync) <- client.describeConfigs(configResources) <&> configsAsync

            _     <- client.deleteTopics(List("topic6", "topic7"))
            list3 <- client.listTopics()
          } yield assert(list1.size)(equalTo(0)) &&
            assert(configs.size)(equalTo(2)) &&
            assert(awaitedConfigsAsync.size)(equalTo(2)) &&
            assert(list3.size)(equalTo(0))
        }
      },
      testM("list cluster nodes") {
        KafkaTestUtils.withAdmin { client =>
          for {
            nodes <- client.describeClusterNodes()
          } yield assert(nodes.size)(equalTo(1))
        }
      },
      testM("get cluster controller") {
        KafkaTestUtils.withAdmin { client =>
          for {
            controller <- client.describeClusterController()
          } yield assert(controller.map(_.id))(isSome(equalTo(0)))
        }
      },
      testM("get cluster id") {
        KafkaTestUtils.withAdmin { client =>
          for {
            controllerId <- client.describeClusterId()
          } yield assert(controllerId.nonEmpty)(isTrue)
        }
      },
      testM("get cluster authorized operations") {
        KafkaTestUtils.withAdmin { client =>
          for {
            operations <- client.describeClusterAuthorizedOperations()
          } yield assert(operations)(equalTo(Set.empty[AclOperation]))
        }
      },
      testM("describe broker config") {
        KafkaTestUtils.withAdmin { client =>
          for {
            configs <- client.describeConfigs(
                         List(
                           ConfigResource(ConfigResourceType.Broker, "0")
                         )
                       )
          } yield assert(configs.size)(equalTo(1))
        }
      },
      testM("describe broker config async") {
        KafkaTestUtils.withAdmin { client =>
          for {
            configTasks <- client.describeConfigsAsync(
                             List(
                               ConfigResource(ConfigResourceType.Broker, "0")
                             )
                           )
            configs <- ZIO.foreachPar(configTasks) { case (resource, configTask) =>
                         configTask.map(config => (resource, config))
                       }
          } yield assertTrue(configs.size == 1)
        }
      },
      testM("list offsets") {
        KafkaTestUtils.withAdmin { client =>
          val topic    = "topic8"
          val msgCount = 20
          val kvs      = (1 to msgCount).toList.map(i => (s"key$i", s"msg$i"))

          for {
            _ <- client.createTopics(List(AdminClient.NewTopic("topic8", 3, 1)))
            _ <- produceMany(topic, kvs).provideSomeLayer[Has[Kafka] with Blocking with Clock](producer)
            offsets <- client.listOffsets(
                         (0 until 3).map(i => TopicPartition(topic, i) -> OffsetSpec.LatestSpec).toMap
                       )
          } yield assert(offsets.values.map(_.offset).sum)(equalTo(msgCount.toLong))
        }
      },
      testM("list offsets async") {
        KafkaTestUtils.withAdmin { client =>
          val topic    = "topic9"
          val msgCount = 20
          val kvs      = (1 to msgCount).toList.map(i => (s"key$i", s"msg$i"))

          for {
            _ <- client.createTopics(List(AdminClient.NewTopic("topic9", 3, 1)))
            _ <- produceMany(topic, kvs).provideSomeLayer[Has[Kafka] with Blocking with Clock](producer)
            offsetTasks <- client.listOffsetsAsync(
                             (0 until 3).map(i => TopicPartition(topic, i) -> OffsetSpec.LatestSpec).toMap
                           )
            offsets <- ZIO.foreachPar(offsetTasks) { case (topicPartition, offsetTask) =>
                         offsetTask.map((topicPartition, _))
                       }
          } yield assert(offsets.values.map(_.offset).sum)(equalTo(msgCount.toLong))
        }
      },
      testM("alter offsets") {
        KafkaTestUtils.withAdmin { client =>
          val topic            = "topic10"
          val consumerGroupID  = "topic10"
          val partitionCount   = 3
          val msgCount         = 20
          val partitionResetBy = 2

          val p   = (0 until partitionCount).map(i => TopicPartition("topic10", i))
          val kvs = (1 to msgCount).toList.map(i => (s"key$i", s"msg$i"))

          def consumeAndCommit(count: Long) =
            Consumer
              .subscribeAnd(Subscription.Topics(Set(topic)))
              .partitionedStream[Has[Kafka] with Blocking with Clock, String, String](Serde.string, Serde.string)
              .flatMapPar(partitionCount)(_._2)
              .take(count)
              .transduce(ZTransducer.collectAllN(Int.MaxValue))
              .mapConcatM { committableRecords =>
                val records = committableRecords.map(_.record)
                val offsetBatch =
                  committableRecords.foldLeft(OffsetBatch.empty)(_ merge _.offset)

                offsetBatch.commit.as(records)
              }
              .runCollect
              .provideSomeLayer[Has[Kafka] with Blocking with Clock](consumer("topic10", Some(consumerGroupID)))

          def toMap(records: Chunk[ConsumerRecord[String, String]]): Map[Int, List[(Long, String, String)]] =
            records.toList
              .map(r => (r.partition(), (r.offset(), r.key(), r.value())))
              .groupBy(_._1)
              .map { case (k, vs) => (k, vs.map(_._2).sortBy(_._1)) }

          for {
            _          <- client.createTopics(List(AdminClient.NewTopic(topic, partitionCount, 1)))
            _          <- produceMany(topic, kvs).provideSomeLayer[Has[Kafka] with Blocking with Clock](producer)
            records    <- consumeAndCommit(msgCount.toLong).map(toMap)
            endOffsets <- client.listOffsets((0 until partitionCount).map(i => p(i) -> OffsetSpec.LatestSpec).toMap)
            _ <- client.alterConsumerGroupOffsets(
                   consumerGroupID,
                   Map(
                     p(0) -> OffsetAndMetadata(0),                                         // from the beginning
                     p(1) -> OffsetAndMetadata(endOffsets(p(1)).offset - partitionResetBy) // re-read two messages
                   )
                 )
            expectedMsgsToConsume = endOffsets(p(0)).offset + partitionResetBy
            recordsAfterAltering <- consumeAndCommit(expectedMsgsToConsume.toLong).map(toMap)
          } yield assert(recordsAfterAltering(0))(equalTo(records(0))) &&
            assert(records(1).take(endOffsets(p(1)).offset.toInt - partitionResetBy) ++ recordsAfterAltering(1))(
              equalTo(records(1))
            ) &&
            assert(recordsAfterAltering.get(2))(isNone)
        }
      },
      testM("create, produce, delete records from a topic") {
        KafkaTestUtils.withAdmin { client =>
          type Env = Has[Kafka] with Blocking with Clock
          val topicName      = UUID.randomUUID().toString
          val topicPartition = TopicPartition(topicName, 0)

          for {
            _             <- client.createTopic(AdminClient.NewTopic(topicName, 1, 1))
            _             <- produceOne(topicName, "key", "message").provideSomeLayer[Env](producer)
            offsetsBefore <- client.listOffsets(Map(topicPartition -> OffsetSpec.EarliestSpec))
            _             <- client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(1L)))
            offsetsAfter  <- client.listOffsets(Map(topicPartition -> OffsetSpec.EarliestSpec))
            _             <- client.deleteTopic(topicName)
          } yield assert(offsetsBefore.get(topicPartition).map(_.offset))(isSome(equalTo(0L))) &&
            assert(offsetsAfter.get(topicPartition).map(_.offset))(isSome(equalTo(1L)))

        }
      },
      testM("list consumer groups") {
        KafkaTestUtils.withAdmin { implicit admin =>
          for {
            topicName <- randomTopic
            _         <- admin.createTopic(AdminClient.NewTopic(topicName, numPartitions = 10, replicationFactor = 1))
            groupId   <- randomGroup
            _         <- consumeNoop(topicName, groupId, "consumer1", Some("instance1")).fork
            _         <- getStableConsumerGroupDescription(groupId)
            list      <- admin.listConsumerGroups(Some(ListConsumerGroupsOptions(Set(ConsumerGroupState.Stable))))
          } yield assert(list)(exists(hasField("groupId", _.groupId, equalTo(groupId))))
        }
      },
      testM("list consumer group offsets") {

        def consumeAndCommit(count: Long, topic: String, groupId: String) =
          Consumer
            .subscribeAnd(Subscription.Topics(Set(topic)))
            .plainStream[Has[Kafka] with Blocking with Clock, String, String](Serde.string, Serde.string)
            .take(count)
            .foreach(_.offset.commit)
            .provideSomeLayer[Has[Kafka] with Blocking with Clock](consumer(topic, Some(groupId)))

        KafkaTestUtils.withAdmin { client =>
          for {
            topic          <- randomTopic
            groupId        <- randomGroup
            invalidTopic   <- randomTopic
            invalidGroupId <- randomGroup
            msgCount   = 20
            msgConsume = 15
            kvs        = (1 to msgCount).toList.map(i => (s"key$i", s"msg$i"))
            _ <- client.createTopics(List(AdminClient.NewTopic(topic, 1, 1)))
            _ <- produceMany(topic, kvs).provideSomeLayer[Has[Kafka] with Blocking](producer)
            _ <- consumeAndCommit(msgConsume.toLong, topic, groupId)
            offsets <- client.listConsumerGroupOffsets(
                         groupId,
                         Some(ListConsumerGroupOffsetsOptions(Chunk.single(TopicPartition(topic, 0))))
                       )
            invalidTopicOffsets <- client.listConsumerGroupOffsets(
                                     groupId,
                                     Some(
                                       ListConsumerGroupOffsetsOptions(Chunk.single(TopicPartition(invalidTopic, 0)))
                                     )
                                   )
            invalidTpOffsets <- client.listConsumerGroupOffsets(
                                  groupId,
                                  Some(
                                    ListConsumerGroupOffsetsOptions(Chunk.single(TopicPartition(topic, 1)))
                                  )
                                )
            invalidGroupIdOffsets <- client.listConsumerGroupOffsets(
                                       invalidGroupId,
                                       Some(ListConsumerGroupOffsetsOptions(Chunk.single(TopicPartition(topic, 0))))
                                     )
          } yield assert(offsets.get(TopicPartition(topic, 0)).map(_.offset))(isSome(equalTo(msgConsume.toLong))) &&
            assert(invalidTopicOffsets)(isEmpty) &&
            assert(invalidTpOffsets)(isEmpty) &&
            assert(invalidGroupIdOffsets)(isEmpty)
        }
      },
      testM("describe consumer groups") {
        KafkaTestUtils.withAdmin { implicit admin =>
          for {
            topicName   <- randomTopic
            _           <- admin.createTopic(AdminClient.NewTopic(topicName, numPartitions = 10, replicationFactor = 1))
            groupId     <- randomGroup
            _           <- consumeNoop(topicName, groupId, "consumer1").fork
            _           <- consumeNoop(topicName, groupId, "consumer2").fork
            description <- getStableConsumerGroupDescription(groupId)
          } yield assert(description.groupId)(equalTo(groupId)) && assert(description.members.length)(equalTo(2))
        }
      },
      testM("remove members from consumer groups") {
        KafkaTestUtils.withAdmin { implicit admin =>
          for {
            topicName   <- randomTopic
            _           <- admin.createTopic(AdminClient.NewTopic(topicName, numPartitions = 10, replicationFactor = 1))
            groupId     <- randomGroup
            _           <- consumeNoop(topicName, groupId, "consumer1", Some("instance1")).fork
            consumer2   <- consumeNoop(topicName, groupId, "consumer2", Some("instance2")).fork
            _           <- getStableConsumerGroupDescription(groupId)
            _           <- consumer2.interrupt
            _           <- admin.removeMembersFromConsumerGroup(groupId, Set("instance2"))
            description <- getStableConsumerGroupDescription(groupId)
          } yield assert(description.groupId)(equalTo(groupId)) && assert(description.members.length)(equalTo(1))
        }
      },
      testM("delete consumer groups") {
        KafkaTestUtils.withAdmin { implicit admin =>
          for {
            topicName <- randomTopic
            _         <- admin.createTopic(AdminClient.NewTopic(topicName, numPartitions = 10, replicationFactor = 1))
            groupId1  <- randomGroup
            _ <- admin.alterConsumerGroupOffsets(groupId1, Map(TopicPartition(topicName, 0) -> OffsetAndMetadata(0)))
            groupId2 <- randomGroup
            _ <- admin.alterConsumerGroupOffsets(groupId2, Map(TopicPartition(topicName, 0) -> OffsetAndMetadata(0)))
            consumerGroupsBefore <- admin.listConsumerGroups()
            _                    <- admin.deleteConsumerGroups(List(groupId1))
            consumerGroupsAfter  <- admin.listConsumerGroups()
          } yield assert(consumerGroupsBefore)(exists(hasField("groupId", _.groupId, equalTo(groupId1)))) &&
            assert(consumerGroupsBefore)(exists(hasField("groupId", _.groupId, equalTo(groupId2)))) &&
            assert(consumerGroupsAfter)(not(exists(hasField("groupId", _.groupId, equalTo(groupId1))))) &&
            assert(consumerGroupsAfter)(exists(hasField("groupId", _.groupId, equalTo(groupId2))))
        }
      },
      testM("describe log dirs") {
        KafkaTestUtils.withAdmin { implicit admin =>
          for {
            topicName <- randomTopic
            _         <- admin.createTopic(AdminClient.NewTopic(topicName, numPartitions = 1, replicationFactor = 1))
            node      <- admin.describeClusterNodes().head.orElseFail(new NoSuchElementException())
            logDirs   <- admin.describeLogDirs(List(node.id))
          } yield assert(logDirs)(
            hasKey(
              node.id,
              hasValues(exists(hasField("replicaInfos", _.replicaInfos, hasKey(TopicPartition(topicName, 0)))))
            )
          )
        }
      },
      testM("describe log dirs async") {
        KafkaTestUtils.withAdmin { implicit admin =>
          for {
            topicName <- randomTopic
            _         <- admin.createTopic(AdminClient.NewTopic(topicName, numPartitions = 1, replicationFactor = 1))
            node      <- admin.describeClusterNodes().head.orElseFail(new NoSuchElementException())
            logDirs <-
              admin.describeLogDirsAsync(List(node.id)).flatMap { descriptions =>
                ZIO.foreachPar(descriptions) { case (brokerId, descriptionAsync) =>
                  descriptionAsync.map(description => (brokerId, description))
                }
              }
          } yield assert(logDirs)(
            hasKey(
              node.id,
              hasValues(exists(hasField("replicaInfos", _.replicaInfos, hasKey(TopicPartition(topicName, 0)))))
            )
          )
        }
      },
      test("should correctly handle no node (null) when converting JNode to Node") {
        assert(AdminClient.Node.apply(null))(isNone)
      },
      test("should correctly handle noNode when converting JNode to Node") {
        assert(AdminClient.Node.apply(JNode.noNode()))(isNone)
      },
      testM("should correctly keep all information when converting a valid jNode to Node") {
        val posIntGen = Gen.int(0, Int.MaxValue)
        check(posIntGen, Gen.string1(Gen.anyChar), posIntGen, Gen.option(Gen.anyString)) { (id, host, port, rack) =>
          val jNode = new JNode(id, host, port, rack.orNull)
          assert(AdminClient.Node.apply(jNode).map(_.asJava))(
            equalTo(Some(jNode))
          )
        }
      },
      testM("will replace invalid port by None") {
        val posIntGen = Gen.int(0, Int.MaxValue)
        check(posIntGen, Gen.string1(Gen.anyChar), Gen.anyInt, Gen.option(Gen.anyString)) { (id, host, port, rack) =>
          val jNode = new JNode(id, host, port, rack.orNull)
          assert(AdminClient.Node.apply(jNode).map(_.port.isEmpty))(
            equalTo(Some(port < 0))
          )
        }
      },
      test("will replace empty host by None") {
        val jNode = new JNode(0, "", 9092, null)
        assert(AdminClient.Node.apply(jNode).map(_.host.isEmpty))(
          equalTo(Some(true))
        )
      },
      testM("incremental alter configs") {
        KafkaTestUtils.withAdmin { implicit admin =>
          for {
            topicName <- randomTopic
            _         <- admin.createTopic(AdminClient.NewTopic(topicName, numPartitions = 1, replicationFactor = 1))

            configEntry    = new ConfigEntry("retention.ms", "1")
            configResource = ConfigResource(ConfigResourceType.Topic, topicName)

            setAlterConfigOp = AlterConfigOp(configEntry, AlterConfigOpType.Set)
            _ <- admin.incrementalAlterConfigs(Map(configResource -> Seq(setAlterConfigOp)), AlterConfigsOptions())
            updatedConfigsWithUpdate <- admin.describeConfigs(Seq(ConfigResource(ConfigResourceType.Topic, topicName)))

            deleteAlterConfigOp = AlterConfigOp(configEntry, AlterConfigOpType.Delete)
            _ <- admin.incrementalAlterConfigs(Map(configResource -> Seq(deleteAlterConfigOp)), AlterConfigsOptions())
            updatedConfigsWithDelete <- admin.describeConfigs(Seq(ConfigResource(ConfigResourceType.Topic, topicName)))
          } yield {
            val updatedRetentionMsConfig =
              updatedConfigsWithUpdate.get(configResource).flatMap(_.entries.get("retention.ms"))
            val deleteRetentionMsConfig =
              updatedConfigsWithDelete.get(configResource).flatMap(_.entries.get("retention.ms"))
            assert(updatedRetentionMsConfig.map(_.value()))(isSome(equalTo("1"))) &&
            assert(updatedRetentionMsConfig.map(_.source()))(isSome(equalTo(ConfigSource.DYNAMIC_TOPIC_CONFIG))) &&
            assert(deleteRetentionMsConfig.map(_.value()))(isSome(equalTo("604800000"))) &&
            assert(deleteRetentionMsConfig.map(_.source()))(isSome(equalTo(ConfigSource.DEFAULT_CONFIG)))
          }
        }
      },
      testM("incremental alter configs async") {
        KafkaTestUtils.withAdmin { implicit admin =>
          for {
            topicName <- randomTopic
            _         <- admin.createTopic(AdminClient.NewTopic(topicName, numPartitions = 1, replicationFactor = 1))

            configEntry    = new ConfigEntry("retention.ms", "1")
            configResource = ConfigResource(ConfigResourceType.Topic, topicName)

            setAlterConfigOp = AlterConfigOp(configEntry, AlterConfigOpType.Set)
            setResult <-
              admin
                .incrementalAlterConfigsAsync(Map(configResource -> Seq(setAlterConfigOp)), AlterConfigsOptions())
                .flatMap { configsAsync =>
                  ZIO.foreachPar(configsAsync) { case (configResource, unitAsync) =>
                    unitAsync.map(unit => (configResource, unit))
                  }
                }
            updatedConfigsWithUpdate <- admin.describeConfigs(Seq(ConfigResource(ConfigResourceType.Topic, topicName)))

            deleteAlterConfigOp = AlterConfigOp(configEntry, AlterConfigOpType.Delete)
            deleteResult <-
              admin
                .incrementalAlterConfigsAsync(Map(configResource -> Seq(deleteAlterConfigOp)), AlterConfigsOptions())
                .flatMap { configsAsync =>
                  ZIO.foreachPar(configsAsync) { case (configResource, unitAsync) =>
                    unitAsync.map(unit => (configResource, unit))
                  }
                }
            updatedConfigsWithDelete <- admin.describeConfigs(Seq(ConfigResource(ConfigResourceType.Topic, topicName)))
          } yield {
            val updatedRetentionMsConfig =
              updatedConfigsWithUpdate.get(configResource).flatMap(_.entries.get("retention.ms"))
            val deleteRetentionMsConfig =
              updatedConfigsWithDelete.get(configResource).flatMap(_.entries.get("retention.ms"))
            assert(updatedRetentionMsConfig.map(_.value()))(isSome(equalTo("1"))) &&
            assert(updatedRetentionMsConfig.map(_.source()))(isSome(equalTo(ConfigSource.DYNAMIC_TOPIC_CONFIG))) &&
            assert(deleteRetentionMsConfig.map(_.value()))(isSome(equalTo("604800000"))) &&
            assert(deleteRetentionMsConfig.map(_.source()))(isSome(equalTo(ConfigSource.DEFAULT_CONFIG))) &&
            assert(setResult)(equalTo(Map((configResource, ())))) &&
            assert(deleteResult)(equalTo(Map((configResource, ()))))
          }
        }
      },
      testM("alter configs") {
        KafkaTestUtils.withAdmin { implicit admin =>
          for {
            topicName <- randomTopic
            _         <- admin.createTopic(AdminClient.NewTopic(topicName, numPartitions = 1, replicationFactor = 1))

            configEntry    = new ConfigEntry("retention.ms", "1")
            configResource = ConfigResource(ConfigResourceType.Topic, topicName)

            kafkaConfig = KafkaConfig(Map(topicName -> configEntry))
            _                        <- admin.alterConfigs(Map(configResource -> kafkaConfig), AlterConfigsOptions())
            updatedConfigsWithUpdate <- admin.describeConfigs(Seq(ConfigResource(ConfigResourceType.Topic, topicName)))

            emptyKafkaConfig = KafkaConfig(Map.empty[String, ConfigEntry])
            _ <- admin.alterConfigs(Map(configResource -> emptyKafkaConfig), AlterConfigsOptions())
            updatedConfigsWithDelete <- admin.describeConfigs(Seq(ConfigResource(ConfigResourceType.Topic, topicName)))
          } yield {
            val updatedRetentionMsConfig =
              updatedConfigsWithUpdate.get(configResource).flatMap(_.entries.get("retention.ms"))
            val deleteRetentionMsConfig =
              updatedConfigsWithDelete.get(configResource).flatMap(_.entries.get("retention.ms"))
            assert(updatedRetentionMsConfig.map(_.value()))(isSome(equalTo("1"))) &&
            assert(updatedRetentionMsConfig.map(_.source()))(isSome(equalTo(ConfigSource.DYNAMIC_TOPIC_CONFIG))) &&
            assert(deleteRetentionMsConfig.map(_.value()))(isSome(equalTo("604800000"))) &&
            assert(deleteRetentionMsConfig.map(_.source()))(isSome(equalTo(ConfigSource.DEFAULT_CONFIG)))
          }
        }
      },
      testM("alter configs async") {
        KafkaTestUtils.withAdmin { implicit admin =>
          for {
            topicName <- randomTopic
            _         <- admin.createTopic(AdminClient.NewTopic(topicName, numPartitions = 1, replicationFactor = 1))

            configEntry    = new ConfigEntry("retention.ms", "1")
            configResource = ConfigResource(ConfigResourceType.Topic, topicName)

            kafkaConfig = KafkaConfig(Map(topicName -> configEntry))
            setResult <-
              admin
                .alterConfigsAsync(Map(configResource -> kafkaConfig), AlterConfigsOptions())
                .flatMap { configsAsync =>
                  ZIO.foreachPar(configsAsync) { case (configResource, unitAsync) =>
                    unitAsync.map(unit => (configResource, unit))
                  }
                }
            updatedConfigsWithUpdate <- admin.describeConfigs(Seq(ConfigResource(ConfigResourceType.Topic, topicName)))

            emptyKafkaConfig = KafkaConfig(Map.empty[String, ConfigEntry])
            deleteResult <-
              admin
                .alterConfigsAsync(Map(configResource -> emptyKafkaConfig), AlterConfigsOptions())
                .flatMap { configsAsync =>
                  ZIO.foreachPar(configsAsync) { case (configResource, unitAsync) =>
                    unitAsync.map(unit => (configResource, unit))
                  }
                }
            updatedConfigsWithDelete <- admin.describeConfigs(Seq(ConfigResource(ConfigResourceType.Topic, topicName)))
          } yield {
            val updatedRetentionMsConfig =
              updatedConfigsWithUpdate.get(configResource).flatMap(_.entries.get("retention.ms"))
            val deleteRetentionMsConfig =
              updatedConfigsWithDelete.get(configResource).flatMap(_.entries.get("retention.ms"))
            assert(updatedRetentionMsConfig.map(_.value()))(isSome(equalTo("1"))) &&
            assert(updatedRetentionMsConfig.map(_.source()))(isSome(equalTo(ConfigSource.DYNAMIC_TOPIC_CONFIG))) &&
            assert(deleteRetentionMsConfig.map(_.value()))(isSome(equalTo("604800000"))) &&
            assert(deleteRetentionMsConfig.map(_.source()))(isSome(equalTo(ConfigSource.DEFAULT_CONFIG))) &&
            assert(setResult)(equalTo(Map((configResource, ())))) &&
            assert(deleteResult)(equalTo(Map((configResource, ()))))
          }
        }
      }
    ).provideSomeLayerShared[TestEnvironment](Kafka.embedded.mapError(TestFailure.fail) ++ Clock.live) @@ sequential

  private def consumeNoop(
    topicName: String,
    groupId: String,
    clientId: String,
    groupInstanceId: Option[String] = None
  ): ZIO[Has[Kafka] with Clock with Blocking, Throwable, Unit] = Consumer
    .subscribeAnd(Subscription.topics(topicName))
    .plainStream(Serde.string, Serde.string)
    .foreach(_.offset.commit)
    .provideSomeLayer(consumer(clientId, Some(groupId), groupInstanceId))

  private def getStableConsumerGroupDescription(
    groupId: String
  )(implicit adminClient: AdminClient): ZIO[Clock, Throwable, ConsumerGroupDescription] =
    adminClient
      .describeConsumerGroups(groupId)
      .map(_.head._2)
      .repeat(
        (Schedule.recurs(5) && Schedule.fixed(Duration.fromMillis(500)) && Schedule
          .recurUntil[ConsumerGroupDescription](
            _.state == AdminClient.ConsumerGroupState.Stable
          )).map(_._2)
      )
      .flatMap(desc =>
        if (desc.state == AdminClient.ConsumerGroupState.Stable) {
          ZIO.succeed(desc)
        } else {
          ZIO.fail(new IllegalStateException(s"Client is not in stable state: $desc"))
        }
      )
}
