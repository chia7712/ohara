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

package oharastream.ohara.agent

import java.io.ByteArrayOutputStream
import java.nio.charset.{Charset, StandardCharsets}
import java.rmi.RemoteException
import java.util.Objects
import java.util.concurrent.TimeUnit

import oharastream.ohara.common.util.{CommonUtils, Releasable}
import org.apache.sshd.client.SshClient
import org.apache.sshd.client.session.ClientSession
import org.apache.sshd.client.simple.SimpleClient

import scala.concurrent.duration.Duration

/**
  * represent a remote node. Default implementation is based on ssh.
  */
trait Agent extends Releasable {
  /**
    * @return the hostname of remote node
    */
  def hostname: String

  /**
    * @return the user used to login
    */
  def user: String

  /**
    * execute the command by ssh
    * @param command command
    * @return response from remote node
    */
  def execute(command: String): Option[String]

  /**
    * @return true if this object is still available. Otherwise, all executions produce exception
    */
  def isOpen: Boolean
}

/**
  * used to execute command on remote node.
  * the default implementation is based on ssh
  */
object Agent {
  def builder: Builder = new Builder()

  class Builder private[agent] extends oharastream.ohara.common.pattern.Builder[Agent] {
    private[this] var hostname: String  = _
    private[this] var port: Int         = 22
    private[this] var user: String      = _
    private[this] var password: String  = _
    private[this] var timeout: Duration = Duration(3, TimeUnit.SECONDS)
    private[this] var charset           = StandardCharsets.US_ASCII

    /**
      * set remote hostname
      * @param hostname hostname
      * @return this builder
      */
    def hostname(hostname: String): Builder = {
      this.hostname = CommonUtils.requireNonEmpty(hostname)
      this
    }

    /**
      * set remote's ssh port
      * @param port ssh port
      * @return this builder
      */
    def port(port: Int): Builder = {
      this.port = CommonUtils.requireConnectionPort(port)
      this
    }

    /**
      * set remote user name
      * @param user user name
      * @return this builder
      */
    def user(user: String): Builder = {
      this.user = CommonUtils.requireNonEmpty(user)
      this
    }

    /**
      * set remote's ssh password
      * @param password ssh password
      * @return this builder
      */
    def password(password: String): Builder = {
      this.password = CommonUtils.requireNonEmpty(password)
      this
    }

    /**
      * set remote's ssh connection timeout
      * @param timeout ssh timeout duration
      * @return this builder
      */
    def timeout(timeout: Duration): Builder = {
      this.timeout = Objects.requireNonNull(timeout)
      this
    }

    /**
      * set charset in communication
      * @param charset ssh charset
      * @return this builder
      */
    def charset(charset: Charset): Builder = {
      this.charset = Objects.requireNonNull(charset)
      this
    }

    override def build: Agent = new Agent {
      override val hostname: String          = CommonUtils.requireNonEmpty(Builder.this.hostname)
      private[this] val port: Int            = CommonUtils.requireConnectionPort(Builder.this.port)
      override val user: String              = CommonUtils.requireNonEmpty(Builder.this.user)
      private[this] val password: String     = CommonUtils.requireNonEmpty(Builder.this.password)
      private[this] val charset: Charset     = Objects.requireNonNull(Builder.this.charset)
      private[this] var client: SimpleClient = createClient()

      private[this] def createClient(): SimpleClient = {
        val client = SshClient.setUpDefaultSimpleClient()
        client.setAuthenticationTimeout(timeout.toMillis)
        client.setConnectTimeout(timeout.toMillis)
        client
      }

      private[this] def getOrCreateClient(): SimpleClient = {
        if (!isOpen) client = createClient()
        client
      }

      /**
        * try to create a session to remote node. Noted: it may fail to connect to remote node due to connection issue
        * and it is fine to give a retry.
        *
        * @return session
        */
      private[this] def session(): ClientSession = {
        try getOrCreateClient().sessionLogin(hostname, port, user, password)
        catch {
          case e: IllegalStateException if e.getMessage.contains("SshClient not started") =>
            Releasable.close(client)
            client = null
            // try again
            getOrCreateClient().sessionLogin(hostname, port, user, password)
        }
      }

      override def execute(command: String): Option[String] = {
        val stdOut = new ByteArrayOutputStream
        try {
          val stdError = new ByteArrayOutputStream
          def response(): Option[String] =
            if (stdOut.size() != 0) Some(new String(stdOut.toByteArray, charset))
            else if (stdError.size() != 0) Some(new String(stdError.toByteArray, charset))
            else None
          try {
            val s = session()
            try {
              s.executeRemoteCommand(command, stdOut, stdError, charset)
              response()
            } catch {
              case e: Throwable =>
                throw new RemoteException(s"receive error message: ${response().getOrElse("")}", e)
            } finally s.close()
          } finally stdError.close()
        } finally stdOut.close()
      }

      override def close(): Unit = {
        Releasable.close(client)
        client = null
      }

      override def toString: String = s"$user@$hostname:$port"

      override def isOpen: Boolean = client != null && client.isOpen
    }
  }
}
