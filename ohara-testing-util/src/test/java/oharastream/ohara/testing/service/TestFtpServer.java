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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFtpServer extends OharaTest {

  @Test
  public void nullUser() {
    Assertions.assertThrows(
        NullPointerException.class, () -> FtpServer.builder().user(null).build());
  }

  @Test
  public void emptyUser() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> FtpServer.builder().user("").build());
  }

  @Test
  public void nullPassword() {
    Assertions.assertThrows(
        NullPointerException.class, () -> FtpServer.builder().password(null).build());
  }

  @Test
  public void emptyPassword() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> FtpServer.builder().password("").build());
  }

  @Test
  public void nullDataPorts() {
    Assertions.assertThrows(
        NullPointerException.class, () -> FtpServer.builder().dataPorts(null).build());
  }

  @Test
  public void emptyDataPorts() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> FtpServer.builder().dataPorts(List.of()).build());
  }

  @Test
  public void negativeControlPort() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> FtpServer.builder().controlPort(-1).build());
  }

  @Test
  public void negativeDataPort() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> FtpServer.builder().dataPorts(List.of(-1)).build());
  }

  @Test
  public void nullAdvertisedHostname() {
    Assertions.assertThrows(
        NullPointerException.class, () -> FtpServer.builder().advertisedHostname(null).build());
  }

  @Test
  public void nullHomeFolder() {
    Assertions.assertThrows(
        NullPointerException.class, () -> FtpServer.builder().homeFolder(null).build());
  }

  @Test
  public void testSpecificDataPort() {
    int port = CommonUtils.availablePort();
    try (FtpServer ftpServer = FtpServer.builder().dataPorts(List.of(port)).build()) {
      Assertions.assertEquals(port, (int) ftpServer.dataPorts().get(0));
    }
  }

  @Test
  public void testRandomDataPort() {
    try (FtpServer ftpServer = FtpServer.builder().build()) {
      Assertions.assertFalse(ftpServer.dataPorts().isEmpty());
      ftpServer.dataPorts().forEach(p -> Assertions.assertNotEquals(0, (long) p));
    }
  }

  @Test
  public void testSpecificControlPort() {
    int port = CommonUtils.availablePort();
    try (FtpServer ftpServer = FtpServer.builder().controlPort(port).build()) {
      Assertions.assertEquals(port, ftpServer.port());
    }
  }

  @Test
  public void testRandomControlPort() {
    List<Integer> dataPorts = List.of(0);
    try (FtpServer ftpServer = FtpServer.builder().controlPort(0).dataPorts(dataPorts).build()) {
      Assertions.assertNotEquals(0, ftpServer.port());
    }
  }

  @Test
  public void testTtl() throws InterruptedException {
    int ttl = 3;
    ExecutorService es = Executors.newSingleThreadExecutor();
    try {
      es.execute(
          () -> {
            try {
              FtpServer.start(
                  new String[] {
                    FtpServer.CONTROL_PORT, String.valueOf(CommonUtils.availablePort()),
                    FtpServer.DATA_PORTS, String.valueOf(CommonUtils.availablePort()),
                    FtpServer.TTL, String.valueOf(ttl)
                  },
                  ftp -> {});
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
    } finally {
      es.shutdown();
      Assertions.assertTrue(es.awaitTermination(ttl * 2, TimeUnit.SECONDS));
    }
  }

  @Test
  public void defaultMain() throws InterruptedException {
    ExecutorService es = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch latch = new CountDownLatch(1);
      es.execute(
          () -> {
            try {
              FtpServer.start(
                  new String[0],
                  ftp -> {
                    latch.countDown();
                  });
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
      Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    } finally {
      es.shutdownNow();
    }
  }

  @Test
  public void testInputs() throws InterruptedException {
    String user = CommonUtils.randomString(5);
    String password = CommonUtils.randomString(5);
    int controlPort = CommonUtils.availablePort();
    List<Integer> dataPorts =
        IntStream.range(0, 3)
            .map(i -> CommonUtils.availablePort())
            .boxed()
            .collect(Collectors.toUnmodifiableList());
    int ttl = 3;
    ExecutorService es = Executors.newSingleThreadExecutor();
    try {
      es.execute(
          () -> {
            try {
              FtpServer.start(
                  new String[] {
                    FtpServer.USER, user,
                    FtpServer.PASSWORD, password,
                    FtpServer.CONTROL_PORT, String.valueOf(controlPort),
                    FtpServer.DATA_PORTS,
                        dataPorts.stream().map(String::valueOf).collect(Collectors.joining(",")),
                    FtpServer.TTL, String.valueOf(ttl)
                  },
                  ftp -> {
                    Assertions.assertEquals(ftp.user(), user);
                    Assertions.assertEquals(ftp.password(), password);
                    Assertions.assertEquals(ftp.port(), controlPort);
                    Assertions.assertEquals(ftp.dataPorts(), dataPorts);
                  });
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
    } finally {
      es.shutdown();
      Assertions.assertTrue(es.awaitTermination(ttl * 2, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testInputs2() throws InterruptedException {
    String user = CommonUtils.randomString(5);
    String password = CommonUtils.randomString(5);
    int controlPort = CommonUtils.availablePort();
    int portRange = 2;
    int p = CommonUtils.availablePort();
    List<Integer> dataPorts =
        IntStream.range(p, p + portRange).boxed().collect(Collectors.toUnmodifiableList());
    int ttl = 3;
    ExecutorService es = Executors.newSingleThreadExecutor();
    try {
      es.execute(
          () -> {
            try {
              FtpServer.start(
                  new String[] {
                    FtpServer.USER,
                    user,
                    FtpServer.PASSWORD,
                    password,
                    FtpServer.CONTROL_PORT,
                    String.valueOf(controlPort),
                    FtpServer.DATA_PORTS,
                    p + "-" + (p + portRange),
                    FtpServer.TTL,
                    String.valueOf(ttl)
                  },
                  ftp -> {
                    Assertions.assertEquals(ftp.user(), user);
                    Assertions.assertEquals(ftp.password(), password);
                    Assertions.assertEquals(ftp.port(), controlPort);
                    Assertions.assertEquals(ftp.dataPorts(), dataPorts);
                  });
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
    } finally {
      es.shutdown();
      Assertions.assertTrue(es.awaitTermination(ttl * 2, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testHomeFolder() {
    String prefix = CommonUtils.randomString(5);
    File f = CommonUtils.createTempFolder(prefix);
    Assertions.assertTrue(f.delete());
    Assertions.assertFalse(f.exists());
    try (FtpServer ftpServer = FtpServer.builder().homeFolder(f).build()) {
      Assertions.assertTrue(ftpServer.isLocal());
      Assertions.assertTrue(f.exists());
      Assertions.assertEquals(ftpServer.absolutePath(), f.getAbsolutePath());
    }
  }

  @Test
  public void testPortOfLocal() {
    try (FtpServer ftpServer = FtpServer.local()) {
      Assertions.assertNotEquals(0, ftpServer.port());
    }
  }
}
