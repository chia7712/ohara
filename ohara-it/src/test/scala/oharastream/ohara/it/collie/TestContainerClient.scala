/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oharastream.ohara.it.collie

import java.util.concurrent.TimeUnit

import oharastream.ohara.agent.DataCollie
import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.agent.docker.DockerClient
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.it.{ContainerPlatform, IntegrationTest}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

@EnabledIfEnvironmentVariable(named = "ohara.it.docker", matches = ".*")
class TestContainerClient extends IntegrationTest {
  private[this] val name      = CommonUtils.randomString(5)
  private[this] val imageName = "centos:7"
  private[this] val webHost   = "www.google.com.tw"

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testLog(platform: ContainerPlatform): Unit =
    close(platform.setupContainerClient()) { containerClient =>
      def log(name: String, sinceSeconds: Option[Long]): String =
        result(containerClient.log(name, sinceSeconds)).head._2
      def createBusyBox(arguments: Seq[String]): Unit =
        result(
          containerClient.containerCreator
            .nodeName(platform.nodeNames.head)
            .name(name)
            .imageName("busybox")
            .arguments(arguments)
            .create()
        )
      createBusyBox(Seq("sh", "-c", "while true; do $(echo date); sleep 1; done"))
      try {
        // wait the container
        await(() => log(name, None).contains("UTC"))
        val lastLine = log(name, None).split("\n").last
        TimeUnit.SECONDS.sleep(3)
        log(name, Some(1)) should not include lastLine
        log(name, Some(10)) should include(lastLine)
      } finally Releasable.close(() => result(containerClient.forceRemove(name)))
    }(containerClient => result(containerClient.forceRemove(name)))

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testVolume(platform: ContainerPlatform): Unit =
    close(platform.setupContainerClient()) { containerClient =>
      val path  = s"/tmp/${CommonUtils.randomString(10)}"
      val names = Seq(CommonUtils.randomString(), CommonUtils.randomString())
      checkVolumeNotExists(containerClient, names)
      try {
        names.foreach(
          name =>
            result(
              containerClient.volumeCreator
                .name(name)
                .nodeName(platform.nodeNames.head)
                .path(path)
                .create()
            )
        )
        names.foreach { name =>
          result(containerClient.volumes(name)).head.path shouldBe path
          result(containerClient.volumes(name)).head.name shouldBe name
          result(containerClient.volumes(name)).head.nodeName shouldBe platform.nodeNames.head
        }
      } finally {
        names.foreach(name => Releasable.close(() => result(containerClient.removeVolumes(name))))
        checkVolumeNotExists(containerClient, names)
      }
    }(containerClient => result(containerClient.forceRemove(name)))

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testVolumeMultiNode(platform: ContainerPlatform): Unit = {
    val nodeVolumes = platform.nodeNames.map { nodeName =>
      nodeName -> s"volume-${CommonUtils.randomString(5)}"
    }
    val volumeNames = nodeVolumes.map(_._2).toSeq
    close(platform.setupContainerClient()) { containerClient =>
      val path = s"/tmp/${CommonUtils.randomString(10)}"
      checkVolumeNotExists(containerClient, volumeNames)
      try {
        nodeVolumes.foreach {
          case (nodeName, volumeName) =>
            result(
              containerClient.volumeCreator
                .name(volumeName)
                .nodeName(nodeName)
                .path(path)
                .create()
            )
        }
        nodeVolumes.foreach {
          case (nodeName, volumeName) =>
            result(containerClient.volumes(volumeName)).foreach { volume =>
              volume.name shouldBe volumeName
              volume.path shouldBe path
              volume.nodeName shouldBe nodeName
            }
        }
      } finally {
        volumeNames.foreach(name => Releasable.close(() => result(containerClient.removeVolumes(name))))
        checkVolumeNotExists(containerClient, volumeNames)
      }
    }(containerClient => volumeNames.foreach(volumeName => result(containerClient.forceRemove(volumeName))))
  }

  private[this] def checkVolumeNotExists(containerClient: ContainerClient, names: Seq[String]): Unit =
    names.foreach { volumeName =>
      await(() => !result(containerClient.volumes()).exists(_.name == volumeName))
    }

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testList(platform: ContainerPlatform): Unit =
    close(platform.setupContainerClient()) { containerClient =>
      result(
        containerClient.containerCreator
          .nodeName(platform.nodeNames.head)
          .name(name)
          .imageName(imageName)
          .command(s"""/bin/bash -c \"ping $webHost\"""")
          .create()
      )
      result(containerClient.containerNames()).map(_.name) should contain(name)
    }(containerClient => result(containerClient.forceRemove(name)))

  def testRoute(): Unit = {
    val platform = ContainerPlatform.dockerMode
    close(platform.setupContainerClient()) { containerClient =>
      result(
        containerClient.containerCreator
          .nodeName(platform.nodeNames.head)
          .name(name)
          .routes(Map("ABC" -> "192.168.123.123"))
          .imageName(imageName)
          .command(s"""/bin/bash -c \"ping $webHost\"""")
          .create()
      )
      val hostFile =
        result(containerClient.asInstanceOf[DockerClient].containerInspector.name(name).cat("/etc/hosts")).head._2
      hostFile should include("192.168.123.123")
      hostFile should include("ABC")
    }(containerClient => result(containerClient.forceRemove(name)))
  }

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testPortMapping(platform: ContainerPlatform): Unit =
    close(platform.setupContainerClient()) { containerClient =>
      val availablePort = CommonUtils.availablePort()
      result(
        containerClient.containerCreator
          .nodeName(platform.nodeNames.head)
          .name(name)
          .imageName(imageName)
          .portMappings(Map(availablePort -> availablePort))
          .command(s"""/bin/bash -c \"ping $webHost\"""")
          .create()
      )

      val container = result(containerClient.containers()).find(_.name == name).get
      container.portMappings.size shouldBe 1
      container.portMappings.size shouldBe 1
      container.portMappings.head.hostPort shouldBe availablePort
      container.portMappings.head.containerPort shouldBe availablePort
    }(containerClient => result(containerClient.forceRemove(name)))

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testSetEnv(platform: ContainerPlatform): Unit =
    close(platform.setupContainerClient()) { containerClient =>
      result(
        containerClient.containerCreator
          .nodeName(platform.nodeNames.head)
          .name(name)
          .imageName(imageName)
          .envs(Map("abc" -> "123", "ccc" -> "ttt"))
          .command(s"""/bin/bash -c \"ping $webHost\"""")
          .create()
      )
      val container = result(containerClient.containers()).find(_.name == name).get
      container.environments("abc") shouldBe "123"
      container.environments("ccc") shouldBe "ttt"
    }(containerClient => result(containerClient.forceRemove(name)))

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testHostname(platform: ContainerPlatform): Unit =
    close(platform.setupContainerClient()) { containerClient =>
      val hostname = CommonUtils.randomString(5)
      result(
        containerClient.containerCreator
          .nodeName(platform.nodeNames.head)
          .name(name)
          .imageName(imageName)
          .hostname(hostname)
          .command(s"""/bin/bash -c \"ping $webHost\"""")
          .create()
      )
      result(containerClient.containers()).find(_.name == name).get.hostname shouldBe hostname
    }(containerClient => result(containerClient.forceRemove(name)))

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testNodeName(platform: ContainerPlatform): Unit =
    close(platform.setupContainerClient()) { containerClient =>
      result(
        containerClient.containerCreator
          .nodeName(platform.nodeNames.head)
          .name(name)
          .imageName(imageName)
          .command(s"""/bin/bash -c \"ping $webHost\"""")
          .create()
      )
      result(containerClient.containers()).find(_.name == name).get.nodeName shouldBe platform.nodeNames.head
    }(containerClient => result(containerClient.forceRemove(name)))

  def testAppend(): Unit = {
    val platform = ContainerPlatform.dockerMode
    close(platform.setupContainerClient()) { containerClient =>
      result(
        containerClient.containerCreator
          .nodeName(platform.nodeNames.head)
          .name(name)
          .imageName(imageName)
          .command(s"""/bin/bash -c \"ping $webHost\"""")
          .create()
      )
      val container = result(containerClient.containers()).find(_.name == name).get
      result(
        containerClient.asInstanceOf[DockerClient].containerInspector.name(container.name).append("/tmp/ttt", "abc")
      ).head._2 shouldBe "abc\n"
      result(
        containerClient.asInstanceOf[DockerClient].containerInspector.name(container.name).append("/tmp/ttt", "abc")
      ).head._2 shouldBe "abc\nabc\n"
      result(
        containerClient
          .asInstanceOf[DockerClient]
          .containerInspector
          .name(container.name)
          .append("/tmp/ttt", Seq("t", "z"))
      ).head._2 shouldBe "abc\nabc\nt\nz\n"
    }(containerClient => result(containerClient.forceRemove(name)))
  }

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testResources(platform: ContainerPlatform): Unit =
    close(platform.setupContainerClient()) { containerClient =>
      result(containerClient.resources()) should not be Map.empty
    }(_ => ())

  @Test
  def testResourcesOfUnavailableNode(): Unit =
    close(
      DockerClient(
        DataCollie(
          Seq(
            Node(
              hostname = "abc",
              user = "user",
              password = "password"
            )
          )
        )
      )
    )(containerClient => result(containerClient.resources()) shouldBe Map.empty)(_ => ())
}

object TestContainerClient {
  def parameters: java.util.stream.Stream[Arguments] = ContainerPlatform.all.map(o => Arguments.of(o)).asJava.stream()
}
