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

package oharastream.ohara.testing.service;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;
import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;

/**
 * a simple embedded ftp server providing 1 writable user.
 *
 * <p>all resources will be released by FtpServer#close(). For example, all data in home folder will
 * be deleted
 *
 * <p>If ohara.it.ftp exists in env variables, local ftp server is not created.
 */
public interface FtpServer extends Releasable {
  String hostname();

  int port();

  String user();

  String password();

  /**
   * If the ftp server is in passive mode, the port is used to transfer data
   *
   * @return data port
   */
  List<Integer> dataPorts();

  /** @return true if this ftp server is generated locally. */
  boolean isLocal();

  String absolutePath();

  static Builder builder() {
    return new Builder();
  }

  class Builder implements oharastream.ohara.common.pattern.Builder<FtpServer> {
    private Builder() {}

    private String advertisedHostname = CommonUtils.hostname();
    private File homeFolder = CommonUtils.createTempFolder("local_ftp");
    private String user = "user";
    private String password = "password";
    private int controlPort = 0;
    private List<Integer> dataPorts = Arrays.asList(0, 0, 0);

    @oharastream.ohara.common.annotations.Optional("default is local hostname")
    public Builder homeFolder(File homeFolder) {
      this.homeFolder = Objects.requireNonNull(homeFolder);
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default is local hostname")
    public Builder advertisedHostname(String advertisedHostname) {
      this.advertisedHostname = CommonUtils.requireNonEmpty(advertisedHostname);
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default is user")
    public Builder user(String user) {
      this.user = CommonUtils.requireNonEmpty(user);
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default is password")
    public Builder password(String password) {
      this.password = CommonUtils.requireNonEmpty(password);
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default is random port")
    public Builder controlPort(int controlPort) {
      this.controlPort = CommonUtils.requireBindPort(controlPort);
      return this;
    }

    /**
     * set the ports used to translate data. NOTED: the max connection of data is equal to number of
     * data ports.
     *
     * @param dataPorts data ports
     * @return this builder
     */
    @oharastream.ohara.common.annotations.Optional("default is three random ports")
    public Builder dataPorts(List<Integer> dataPorts) {
      CommonUtils.requireNonEmpty(
              Objects.requireNonNull(dataPorts), () -> "dataPorts can't be empty")
          .forEach(CommonUtils::requireBindPort);
      this.dataPorts = dataPorts;
      return this;
    }

    private void checkArguments() {
      if (!homeFolder.exists() && !homeFolder.mkdir())
        throw new IllegalStateException("fail to create folder on " + homeFolder.getAbsolutePath());
      if (!homeFolder.isDirectory())
        throw new IllegalArgumentException(homeFolder.getAbsolutePath() + " is not folder");
    }

    @Override
    public FtpServer build() {
      checkArguments();
      PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
      UserManager userManager = userManagerFactory.createUserManager();
      BaseUser _user = new BaseUser();
      _user.setName(user);
      _user.setAuthorities(List.of(new WritePermission()));
      _user.setEnabled(true);
      _user.setPassword(password);
      _user.setHomeDirectory(homeFolder.getAbsolutePath());
      try {
        userManager.save(_user);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      ListenerFactory listenerFactory = new ListenerFactory();
      listenerFactory.setPort(controlPort);
      DataConnectionConfigurationFactory connectionConfig =
          new DataConnectionConfigurationFactory();

      List<Integer> availableDataPorts =
          dataPorts.stream().map(CommonUtils::resolvePort).collect(Collectors.toUnmodifiableList());

      connectionConfig.setActiveEnabled(false);
      connectionConfig.setPassiveExternalAddress(advertisedHostname);
      connectionConfig.setPassivePorts(mkPortString(availableDataPorts));
      listenerFactory.setDataConnectionConfiguration(
          connectionConfig.createDataConnectionConfiguration());

      Listener listener = listenerFactory.createListener();
      FtpServerFactory factory = new FtpServerFactory();
      factory.setUserManager(userManager);
      factory.addListener("default", listener);
      ConnectionConfigFactory connectionConfigFactory = new ConnectionConfigFactory();
      // we disallow user to access ftp server by anonymous
      connectionConfigFactory.setAnonymousLoginEnabled(false);
      factory.setConnectionConfig(connectionConfigFactory.createConnectionConfig());
      org.apache.ftpserver.FtpServer server = factory.createServer();
      try {
        server.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return new FtpServer() {

        @Override
        public void close() {
          server.stop();
          CommonUtils.deleteFiles(homeFolder);
        }

        @Override
        public String hostname() {
          return CommonUtils.hostname();
        }

        @Override
        public int port() {
          return listener.getPort();
        }

        @Override
        public String user() {
          return _user.getName();
        }

        @Override
        public String password() {
          return _user.getPassword();
        }

        @Override
        public List<Integer> dataPorts() {
          return Stream.of(connectionConfig.getPassivePorts().split(","))
              .map(Integer::valueOf)
              .collect(Collectors.toUnmodifiableList());
        }

        @Override
        public boolean isLocal() {
          return true;
        }

        @Override
        public String absolutePath() {
          return homeFolder.getAbsolutePath();
        }
      };
    }
  }

  /**
   * create a ftp server on local. It use a random port for handling control command, and three
   * random ports are used to send/receive data to/from client.
   *
   * @return ftp server
   */
  static FtpServer local() {
    return FtpServer.builder().dataPorts(Arrays.asList(0, 0, 0)).build();
  }

  static String mkPortString(List<Integer> ports) {
    return ports.stream().map(String::valueOf).collect(Collectors.joining(","));
  }

  String HOME_FOLDER = "--homeFolder";
  String ADVERTISED_HOSTNAME = "--hostname";
  String USER = "--user";
  String PASSWORD = "--password";
  String CONTROL_PORT = "--controlPort";
  String DATA_PORTS = "--dataPorts";
  String TTL = "--ttl";
  String USAGE =
      String.join(
          " ",
          Arrays.asList(
              HOME_FOLDER,
              ADVERTISED_HOSTNAME,
              USER,
              PASSWORD,
              CONTROL_PORT,
              DATA_PORTS,
              "(form: 12345,12346 or 12345-12346)",
              TTL));

  static void start(String[] args, Consumer<FtpServer> consumer) throws InterruptedException {
    File homeFolder = CommonUtils.createTempFolder(CommonUtils.randomString(5));
    String advertisedHostname = CommonUtils.hostname();
    String user = "user";
    String password = "password";
    int controlPort = 0;
    List<Integer> dataPorts = Arrays.asList(0, 0, 0);
    int ttl = Integer.MAX_VALUE;
    if (args.length % 2 != 0) throw new IllegalArgumentException(USAGE);
    for (int i = 0; i < args.length; i += 2) {
      String value = args[i + 1];
      switch (args[i]) {
        case HOME_FOLDER:
          homeFolder = new File(value);
          break;
        case ADVERTISED_HOSTNAME:
          advertisedHostname = value;
          break;
        case USER:
          user = value;
          break;
        case PASSWORD:
          password = value;
          break;
        case CONTROL_PORT:
          controlPort = Integer.parseInt(value);
          break;
        case DATA_PORTS:
          if (value.startsWith("-"))
            throw new IllegalArgumentException("dataPorts must be positive");
          else if (value.contains("-"))
            dataPorts =
                IntStream.range(
                        Integer.parseInt(value.split("-")[0]),
                        Integer.parseInt(value.split("-")[1]) + 1)
                    .boxed()
                    .collect(Collectors.toUnmodifiableList());
          else
            dataPorts =
                Stream.of(value.split(","))
                    .map(Integer::valueOf)
                    .collect(Collectors.toUnmodifiableList());
          break;
        case TTL:
          ttl = Integer.parseInt(value);
          break;
        default:
          throw new IllegalArgumentException("unknown key:" + args[i] + " " + USAGE);
      }
    }
    try (FtpServer ftp =
        FtpServer.builder()
            .homeFolder(homeFolder)
            .advertisedHostname(advertisedHostname)
            .user(user)
            .password(password)
            .controlPort(controlPort)
            .dataPorts(dataPorts)
            .build()) {
      System.out.println(
          String.join(
              " ",
              Arrays.asList(
                  "user:",
                  ftp.user(),
                  "password:",
                  ftp.password(),
                  "hostname:",
                  ftp.hostname(),
                  "port:",
                  String.valueOf(ftp.port()),
                  "data ports:",
                  ftp.dataPorts().stream().map(String::valueOf).collect(Collectors.joining(",")),
                  "absolutePath:",
                  ftp.absolutePath())));
      consumer.accept(ftp);
      TimeUnit.SECONDS.sleep(ttl);
    }
  }

  static void main(String[] args) throws InterruptedException {
    start(args, ftp -> {});
  }
}
