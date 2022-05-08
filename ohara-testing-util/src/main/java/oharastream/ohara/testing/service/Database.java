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

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.config.Charset.UTF8;
import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static com.wix.mysql.distribution.Version.v5_7_latest;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.MysqldConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.TimeUnit;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;

public interface Database extends Releasable {

  String hostname();

  int port();

  String databaseName();

  String user();

  String password();

  String url();

  Connection connection();

  /** @return true if this database is generated locally. */
  boolean isLocal();

  static Builder builder() {
    return new Builder();
  }

  class Builder implements oharastream.ohara.common.pattern.Builder<Database> {
    private Builder() {}

    private String databaseName = "ohara";
    private String user = "user";
    private String password = "password";
    private int port = 0;

    @oharastream.ohara.common.annotations.Optional("default is ohara")
    public Builder databaseName(String databaseName) {
      this.databaseName = CommonUtils.requireNonEmpty(databaseName);
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
    public Builder port(int port) {
      this.port = CommonUtils.requireBindPort(port);
      return this;
    }

    @Override
    public Database build() {
      MysqldConfig config =
          aMysqldConfig(v5_7_latest)
              .withCharset(UTF8)
              .withUser(user, password)
              .withTimeZone(CommonUtils.timezone())
              .withTimeout(2, TimeUnit.MINUTES)
              .withServerVariable("max_connect_errors", 666)
              .withTempDir(CommonUtils.createTempFolder("local_mysql").getAbsolutePath())
              .withPort(CommonUtils.resolvePort(port))
              // make mysql use " replace '
              // see https://stackoverflow.com/questions/13884854/mysql-double-quoted-table-names
              .withServerVariable("sql-mode", "ANSI_QUOTES")
              .build();
      EmbeddedMysql mysqld = anEmbeddedMysql(config).addSchema(databaseName).start();
      return new Database() {
        private Connection connection = null;

        @Override
        public void close() {
          Releasable.close(connection);
          mysqld.stop();
        }

        @Override
        public String hostname() {
          return CommonUtils.hostname();
        }

        @Override
        public int port() {
          return config.getPort();
        }

        @Override
        public String databaseName() {
          return databaseName;
        }

        @Override
        public String user() {
          return config.getUsername();
        }

        @Override
        public String password() {
          return config.getPassword();
        }

        @Override
        public String url() {
          return "jdbc:mysql://" + hostname() + ":" + port() + "/" + databaseName();
        }

        @Override
        public Connection connection() {
          if (connection == null) {
            try {
              connection = DriverManager.getConnection(url(), user(), password());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          return connection;
        }

        @Override
        public boolean isLocal() {
          return true;
        }
      };
    }
  }

  static Database local() {
    return Database.builder().build();
  }
}
